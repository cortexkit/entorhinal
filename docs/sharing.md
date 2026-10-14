# Sharing between Macs

How to turn on the shared identity log so that projects, workspaces and agents
are the same on each of your Macs, and how to name each Mac. The rules behind
these commands are in [identity-log-contract.md](identity-log-contract.md).

What is shared: projects, workspaces, project placement, root keys, agents and
their names, and Mac display names. What is not: folder paths, bindings,
approvals, owned-remote choices, workspace root folders, sessions, consent
grants, hire caps and move state. Each Mac keeps its own.

The log is stored by engram, so engram must be running and must accept this
Mac as a member of the log. entorhinal does not manage that membership; a Mac
that is not a member gets `identity_log_not_member`.

## Check the current state

```sh
ck projects log status
```

Reads local state only: `state` (`disabled`, `enabling`, `joining` or
`enabled`), `lastAppliedPosition`, `lastSeenHead` and `pendingWriteCount`.

## Enable on the first Mac

1. Make sure agent identity has been imported from the executive. If this
   fleet has no agents at all, you will pass `--without-agents` instead.
2. Enable:

   ```sh
   ck projects log enable
   # or, only when there are no agents to import:
   ck projects log enable --without-agents
   ```

   This writes a snapshot of this Mac's projects, workspaces and agents to the
   log.

Refusals, all before anything changes:

| Code | Meaning |
|---|---|
| `agents_not_imported` | No agents and no import yet. Import first, or pass `--without-agents` if there are truly none. |
| `invalid_request` | `--without-agents` was passed but agents or an import exist. |
| `authority_not_cut_over` | Agents exist but the import was never completed. |
| `join_requires_empty_registry` | The log already has entries from another Mac and this registry is not empty (see below). |
| `engram_unavailable`, `engram_key_unavailable`, `identity_log_not_member` | engram cannot be used from this Mac. |

Verify: `ck projects log status` shows `enabled`, with
`lastAppliedPosition` equal to `lastSeenHead` and `pendingWriteCount` 0.

## Join from another Mac

A Mac joins with an **empty** registry: no projects, workspaces or agents.

```sh
ck projects log enable
```

The Mac reads the log from the start, installs the first Mac's snapshot and
every later entry, and reaches `enabled` once it has caught up. Then give each
shared project a folder here with `ck projects attach`
([projects-and-roots.md](projects-and-roots.md#attach-a-checkout-on-another-mac)).

A Mac that already has a registry cannot join a log another Mac started
(`join_requires_empty_registry`, with the local counts). It can only replace
the log's contents when it wrote every entry in it itself.

If enabling or joining is interrupted, the Mac stays in `enabling` or
`joining` and finishes in the background; running `ck projects log enable`
again resumes it. While it lasts, shared writes refuse
`identity_log_enabling`.

## Name each Mac

Moves target a Mac by its display name, so name every Mac after it is enabled:

```sh
ck projects log name "Studio"
```

The name is trimmed and must be 1–64 characters. A Mac can name only itself.
It refuses `identity_log_disabled` before the log is enabled,
`identity_log_enabling` during bootstrap, and `self_author_unknown` until this
Mac has read its own identity from the log.

Other Macs learn the name at their next catch-up. Reverse: set a different
name.

## Turning sharing off

There is no operation that disables the log once it is enabled.

## When something is wrong

- `pendingWriteCount` above 0 after a command reported
  `engram_outcome_unknown`: the write may or may not have landed. The next
  catch-up (within 30 seconds, or before the next shared write) settles it.
  Retry the command with the printed retry line if it did not land.
- `identity_log_stalled`: catch-up stopped at an entry it could not apply or
  verify, or at a gap. Shared writes refuse until it moves again. The module's
  health shows the reason (`log_gap`, `log_verify_failed`, `apply_failed`,
  `identity_log_invariant`). A log head lower than this Mac has already
  applied is ignored and reported as `log_head_regressed`.
- `identity_log_contended`: other Macs kept appending first; retry.

# Sharing between machines

How to share projects, workspaces and agents across your machines through the
identity log on your NATS hub, and how to name each machine. The rules behind
these commands are in [hub-log-contract.md](hub-log-contract.md).

What is shared: projects, workspaces, project placement, root keys, agents and
their names, and machine display names. What is not: folder paths, bindings,
approvals, owned-remote choices, workspace root folders, sessions, consent
grants, hire caps and local move checkpoints. Each machine keeps its own.

The hub also coordinates agent residence, role leases and agent moves. Those
records are separate from the identity log.

## Check the current state

```sh
ck projects log status
```

Reads local state only: the sharing state, `lastAppliedPosition`,
`lastSeenHead`, `pendingWriteCount`, and any recorded error. See
[Status](hub-log-contract.md#status) for the reply fields.

## Enable on the first machine

1. Make sure agent identity has been imported from the executive (the
   `prefrontal-core` module, which held agent identity before entorhinal). If this
   fleet has no agents at all, you will pass `--without-agents` instead.
2. Enable:

   ```sh
   ck projects log enable
   # or, only when there are no agents to import:
   ck projects log enable --without-agents
   ```

   Confirm the presence prompt. On an empty hub, this writes a snapshot of this
   machine's shared registry. Declining leaves sharing off and publishes nothing.
   A brand-new machine (no projects, workspaces or agents, and an empty hub)
   starts its registry without the prompt, because setup's own confirmation
   covers it; `ck setup` runs this step itself.

Refusals, all before anything changes:

| Code | Meaning |
|---|---|
| `agents_not_imported` | No agents and no import yet. Import first, or pass `--without-agents` if there are truly none. |
| `invalid_request` | `--without-agents` was passed but agents or an import exist. |
| `authority_not_cut_over` | Agents exist but the import was never completed. |
| `join_requires_empty_registry` | The log already has entries from another machine and this registry is not empty (see below). |
| `hub_not_ready` | The hub backend is not ready to enable sharing. |

Verify: `ck projects log status` shows `enabled`, with
`lastAppliedPosition` equal to `lastSeenHead` and `pendingWriteCount` 0.
`pendingWriteCount` counts retained pending rows, not hub write attempts.

## Join from another machine

A machine joins with an **empty** registry: no projects, workspaces or agents.

```sh
ck projects log enable
```

An empty registry can also join automatically when it finds a complete hub
snapshot. Enabling explicitly starts the join without waiting for that check.
The machine installs the snapshot and follows later entries. Give each
shared project a folder here with `ck projects attach`
([projects-and-roots.md](projects-and-roots.md#attach-a-checkout-on-another-machine)).

A non-empty local registry cannot join a populated hub
(`join_requires_empty_registry`, with the local counts). If another machine
starts the hub while the presence prompt is open, this machine checks the hub
again after confirmation and follows the same join rule instead of saving a
bootstrap plan.

If a first-machine enable is interrupted after saving its plan, the machine
finishes that plan in the background, including after a restart. Running
`ck projects log enable` again resumes it. Shared writes refuse
`identity_log_enabling` during bootstrap.

## Name each machine

Moves target a machine by its display name, so name every machine after it is enabled:

```sh
ck projects log name "Studio"
```

The name is trimmed and must be 1–64 characters. A machine can name only itself.
It refuses `identity_log_disabled` before the log is enabled,
`identity_log_enabling` during bootstrap, and `self_author_unknown` until this
machine has read its own identity from the log.

Other machines learn the name through the log tail. Reverse: set a different
name.

## Take a role

`role.take` requires hub sharing and operator confirmation. After checking
the caller and validating the request, it refuses `identity_log_disabled`
when sharing is not enabled, or `hub_not_ready` when sharing is enabled but
the hub backend has not been selected. Neither refusal prompts or writes.
Local role leases still work with sharing off.

## Turning sharing off

There is no operation that disables the log once it is enabled.

## When something is wrong

- `legacy_sharing_unsupported` means this registry store shares through engram.
  This build carries over **neither shared nor local-only data** from that
  store. Move its folder aside and keep it; do not delete it. Start with a new,
  empty registry. If this account's registry already lives on the hub, the
  computer joins it automatically; run `ck projects log enable` to join at once.
  Otherwise, to start a new shared registry, import agents first and run
  `ck projects log enable`, or run `ck projects log enable --without-agents`,
  then confirm the prompt.
- If the hub log is not ready or has stopped advancing, local reads still
  use the last committed state. Check `ck projects log status` and
  `ck health entorhinal` before retrying a shared write.

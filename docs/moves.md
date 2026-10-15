# Moving agents

How to move an idle agent to another of your Macs, follow its progress, cancel
it, and allow or deny moves when a Mac's lid closes. The rules behind these
commands are in [agent-move-contract.md](agent-move-contract.md).

## Before you start

- The shared identity log is enabled on both Macs, and the target Mac has a
  display name ([sharing.md](sharing.md)).
- This Mac holds the agent: `ck agents where <agent>` names this Mac.
- The agent is idle: none of its sessions is running, and every module that
  keeps its data reports it idle.
- Both Macs run entorhinal and the modules that keep agent data.

## Move an agent

```sh
ck agents move Ada --to Studio
```

Approve the move at the Touch ID prompt. The reply carries the move's
`cut_id` and its first step (`preparing`). The rest happens in the background
on both Macs.

Common refusals, before anything is written:

| Code | What to do |
|---|---|
| `agent_busy` | The detail lists each busy session or module and why (`session_active`, `liveness_unknown`, a module's reason). End the sessions and try again. |
| `not_holder`, `claim_stale`, `claim_integrity_failed` | Run `ck agents where <agent> --refresh` and check which Mac holds the agent. |
| `unknown_member` | No Mac has that display name. |
| `invalid_request` | The target is this Mac, or the arguments are malformed. |
| `move_in_progress`, `move_cap` | The agent already has an open move, or this Mac has four. |
| `participant_unavailable` | A module that keeps agent data is missing or not ready. |
| `identity_log_disabled` | Enable the log first. |

If the command reports an unknown outcome, run the printed retry line: it
reuses the request key and returns the existing move instead of starting a
second one.

## Follow a move

```sh
ck agents moves
```

Lists open moves on this Mac, oldest first, then the 32 most recently
finished ones. Each shows the side (`source` or `receiver`), the other Mac,
the step, and any hold, warning or diagnostic. Both Macs list the same move
under the same `cut_id`. A move is finished on the source at `released` (or
`aborted`) and on the receiver at `done` or `taken_over` (or `aborted`).

`agent.move_status {cut_id}` returns one move; there is no separate `ck`
command for it.

Verify the result: `ck agents where <agent> --refresh` names the target Mac.

Things a move can report:

- A receiver participant's `diverged` answer of `unknown` or `advanced` blocks
  import and aborts the move. If the participant supplies a human-readable
  reason, `ck agents moves` shows it next to that participant and ref; the
  reason is also retained in move status and `ck agents where`. No reason is
  shown when the participant did not provide one.
- `release_not_enforced` warning: a module on the old Mac cannot block writes
  to the agent's data. Do not use the agent on the old Mac; anything written
  there becomes a fork.
- `abort_stuck`: a module refused to undo its part of the move. On the source,
  the agent stays frozen. Repair the named module, then retry (below).
- `held_as_fork`: the receiver could not activate the agent because both Macs
  changed it. The move waits for you; entorhinal does not pick a side.
- `contract_violation`: a module replied outside the agent-data contract and
  needs repair.

## Cancel a move

```sh
ck agents move-abort <cut_id>
```

Approve at the Touch ID prompt. Run it on the Mac whose side can still stop:

- On the **receiver**, until it starts activating the agent (steps up to
  `claimed`).
- On the **source**, only while the receiver has not yet staged the move. After
  that, the source's request is cleared and says the receiver must abort.

Once the receiver has started activating the agent, the move always
completes; the command refuses
`invalid_state` and changes nothing. Cancelling an already aborted move, or one
whose cancellation is under way, just shows its status.

Verify: `ck agents moves` shows the move as `aborted` on both Macs, and
`ck agents where <agent> --refresh` names the original Mac.

### Retry a stuck cancellation

When a move shows `abort_stuck`, fix the module named in its `diagnostic`
(`module_id`), then run the same
command again:

```sh
ck agents move-abort <cut_id>
```

The prompt reads "retry stuck abort of move …". Only `ck` (the `Direct`
route) can retry a stuck cancellation. The module is asked once more; if it
still refuses, the move holds again. Restarting entorhinal also retries once.

## Lid-close moves

engram can move an idle agent off a Mac when its lid closes, but only if you
allow it on that Mac:

```sh
ck agents lid-close-moves on
ck agents lid-close-moves off
```

Changing the lid-close setting needs Touch ID; setting it to its current value does not
prompt. The default is off, and the setting is local to each Mac. engram's
moves still pass every check above, including the idle check.

Reverse: run the command with `off` (or `on`).

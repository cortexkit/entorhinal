# Moving agents

How to move an idle agent to another of your machines, follow its progress, cancel
it, and allow or deny moves when a machine's lid closes. The rules behind these
commands are in [agent-move-contract.md](agent-move-contract.md).

## Before you start

- The shared identity log is enabled on both machines, and the target machine has a
  display name ([sharing.md](sharing.md)).
- This machine holds the agent: `ck agents where <agent>` names this machine.
- The agent is idle: none of its sessions is running, and every module that
  keeps its data reports it idle.
- Both machines run entorhinal and the modules that keep agent data.
- Both machines run an entorhinal release that carries session bindings before you
  move an agent that has any session bindings. A machine on an older release
  refuses such a move (`receiver_precondition`, `participant_unavailable`
  naming `session-bindings`) without writing anything, but cannot finish
  cancelling it: its move stays open, retrying, until that machine is upgraded,
  after which it finishes aborting by itself.

## Move an agent

```sh
ck agents move Ada --to Studio
```

Approve the move at the presence prompt. The reply carries the move's
`cut_id` and its first step (`preparing`). The rest happens in the background
on both machines.

Common refusals, before anything is written:

| Code | What to do |
|---|---|
| `agent_busy` | The detail lists each busy session or module and why (`session_active`, `liveness_unknown`, a module's reason). End the sessions and try again. |
| `not_holder`, `claim_stale`, `claim_integrity_failed` | Run `ck agents where <agent> --refresh` and check which machine holds the agent. |
| `unknown_member` | No machine has that display name. |
| `invalid_request` | The target is this machine, or the arguments are malformed. |
| `move_in_progress`, `move_cap` | The agent already has an open move, or this machine has four. |
| `participant_unavailable` | A module that keeps agent data is missing or not ready. |
| `identity_log_disabled` | Enable the log first. |

If the command reports an unknown outcome, run the printed retry line: it
reuses the request key and returns the existing move instead of starting a
second one.

## Follow a move

```sh
ck agents moves
```

Lists open moves on this machine, oldest first, then the 32 most recently
finished ones. Each shows the side (`source` or `receiver`), the other machine,
the step, and any hold, warning or diagnostic. Both machines list the same move
under the same `cut_id`. A move is finished on the source at `released` (or
`aborted`) and on the receiver at `done` or `taken_over` (or `aborted`).

`agent.move_status {cut_id}` returns one move; there is no separate `ck`
command for it.

After activation, the receiving machine releases engram's sealed-cut storage
reservation in the background. After cancellation, the machine that aborted
the cut releases it instead. This cleanup never delays completion or your next
move. The API replies include `cut_release` when cleanup exists: `pending`,
`released` (with `reservationDropped`), or `stopped` (with its refusal code).
`reservationDropped: false` is a successful release: another commit on the
machine still lists the cut. Until engram enables trimming, cleanup may remain
pending and retries every six hours. Ordinary temporary failures retry with
backoff, and restarting entorhinal preserves pending cleanup.

Verify the result: `ck agents where <agent> --refresh` names the target machine.

Things a move can report:

- A receiver participant's `diverged` answer of `unknown` or `advanced` blocks
  import and aborts the move. If the participant supplies a human-readable
  reason, `ck agents moves` shows it next to that participant and ref; the
  reason is also retained in move status and `ck agents where`. No reason is
  shown when the participant did not provide one.
- `release_not_enforced` warning: a module on the old machine cannot block writes
  to the agent's data. Do not use the agent on the old machine; anything written
  there becomes a fork.
- `abort_stuck`: a module refused to undo its part of the move. On the source,
  the agent stays frozen. Repair the named module, then retry (below).
- `held_as_fork`: the receiver could not activate the agent because both machines
  changed it. The move waits for you; entorhinal does not pick a side.
- `contract_violation`: a module replied outside the agent-data contract and
  needs repair.

## Cancel a move

```sh
ck agents move-abort <cut_id>
```

Approve at the presence prompt. Run it on the machine whose side can still stop:

- On the **receiver**, until it starts activating the agent (steps up to
  `claimed`).
- On the **source**, only while the receiver has not yet staged the move. After
  that, the source's request is cleared and says the receiver must abort.

Once the receiver has started activating the agent, the move always
completes; the command refuses
`invalid_state` and changes nothing. Cancelling an already aborted move, or one
whose cancellation is under way, just shows its status.

Verify: `ck agents moves` shows the move as `aborted` on both machines, and
`ck agents where <agent> --refresh` names the original machine.

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

engram can move an idle agent off a machine when its lid closes, but only if you
allow it on that machine:

```sh
ck agents lid-close-moves on
ck agents lid-close-moves off
```

Changing the lid-close setting needs operator confirmation; setting it to its current value does not
prompt. The default is off, and the setting is local to each machine. engram's
moves still pass every check above, including the idle check.

Reverse: run the command with `off` (or `on`).

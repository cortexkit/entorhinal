# Agents

How to create, rename, retag, relabel and retire agents, set the hiring cap,
and correct stored agent workspaces. Agent identity is shared between the
operator's Macs when the identity log is enabled ([sharing.md](sharing.md));
which Mac holds an agent is a separate claim, changed only by a move
([moves.md](moves.md)).

## Before you start

- Agent identity must have been imported from the executive. Until then every
  write refuses `authority_not_cut_over` before any prompt.
- Every write from `ck agents` lands only after you approve the exact change
  at a Touch ID prompt the daemon shows. The command prints
  `a confirmation prompt may appear; approve it to continue` and waits up to
  five minutes. A write that would fail anyway is refused before the prompt.
- `<agent>` is an agent id (`agent_` followed by 8 or 16 hex digits) or a live
  name, optionally `workspace/name`.
- Exit codes: 0 success, 2 transport failure or unknown outcome, 3 refused
  (the message starts with the refusal code), 64 usage error.

### When the outcome is unknown

If the command times out, or the shared log could not confirm the write
(`engram_outcome_unknown`), it prints a retry command, quoted for your shell,
that carries the same `--request-key`. Running it is safe: a key that already
landed returns the stored reply without prompting again, and the key cannot
apply to a different agent or different arguments. For `hire-cap` the retry
command carries no key; setting the same cap again is harmless.

## Roles

| Role (`--role`) | Placement |
|---|---|
| `assistant` | No project or workspace. |
| `workspace-head` | One workspace (`--workspace`). |
| `head` | One project (`--project`); at most one live head per project. |
| `hiree` | One project (`--project`), optionally with `--supervisor <agent>`, which must be the project's live head. |

Names are compared after Unicode normalization (NFC), case folding and
trimming, and must be unique among live agents in their namespace: the
assistant namespace for assistants, the workspace for the others. Invisible
and bidirectional control characters are refused. Labels: at most 16, each
1–32 characters after trimming, unique ignoring case.

## Create

```sh
ck agents create Ada --role head --tag "release work" --project <project-id>
```

Verify: `ck agents show Ada` or `ck agents list --project <project-id>`.

Reverse: `ck agents retire Ada`.

## Rename, retag, relabel

```sh
ck agents rename Ada Ada-2
ck agents tag Ada-2 "docs and release"
ck agents labels Ada-2 docs release      # replaces the whole set
ck agents labels Ada-2 --none            # clears it
```

A prompt summary that is too long for the dialog refuses
`operator_summary_too_long`; shorten the name, tag or labels.

Verify: `ck agents show Ada-2`. Reverse: run the command again with the old
value.

## Retire

```sh
ck agents retire Ada-2
```

The agent becomes `retired`; its id stays reserved and is never reused, and
its name claim is released. Any consent grants it held stop covering requests.

Verify: `ck agents show <agent-id>` shows the retired identity.

Reverse: none. A retired agent cannot be brought back; create a new one.

## Set the hiring cap

Heads can hire agents into their project through the `agents_hire` tool, with
the operator's consent ([consent-contract.md](consent-contract.md)). The cap
limits how many live hirees a project may have before agent-requested hires
are refused.

```sh
ck agents hire-cap <project-id> 4
```

- The value is a non-negative integer; 0 stops agent hiring.
- With no cap set, the cap is 10.
- The count includes every live hiree, however it was created; only hires an
  agent requests are refused at the cap. Lowering the cap retires nobody.
- Caps are local to this Mac and always need Touch ID.

Verify: the command prints the project and the new cap.

Reverse: set the previous value.

## Correct stored agent workspaces

Heads and hirees belong to their project's workspace and should store no
workspace of their own, with their name claim in the project's workspace. An
older registry may hold stored workspaces or claims in the wrong place.

```sh
ck agents repair-workspaces          # preview: what would change, or why it is refused
ck agents repair-workspaces --yes    # send the correction, with Touch ID
```

The preview never writes. If it reports a refusal (for example two agents
whose names would collide in the target workspace), `--yes` will refuse the
same way; resolve the collision first, for example by renaming one agent.

Verify: run the preview again; it reports nothing to change.

Reverse: none as a single command. The change is journaled; rename or move
agents individually if needed.

## Where an agent is

```sh
ck agents where Ada-2              # from the local claim cache
ck agents where Ada-2 --refresh    # refresh claims from engram first
```

The answer says whether the claim is known and which Mac holds it. Reads never
wait for engram unless you pass `--refresh`.

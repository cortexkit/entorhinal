# Agents

How to create, rename, retag, relabel and retire agents, set the hiring cap,
apply owner-minted avatar manifests, and correct stored agent workspaces.
Agent identity is shared between the
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

## Apply avatar genomes

The avatar owner's `avatar-export --mint-genomes` tool produces a JSON array:
each entry has exactly `agent_id`, `avatar_genome`, `avatar_type` and
`avatar_version`. Entorhinal never derives a genome or changes the supplied
string. `avatar_type` names the avatar type; `creature.wild` is one such type, and
its v1 genome is 16,384 bytes (32,768 hex characters).
The type and version belong to each entry, not to the whole file.

```sh
ck agents avatars --manifest avatars.json                 # preview, no writes
ck agents avatars --manifest avatars.json --yes           # seed agents with no avatar
ck agents avatars --manifest avatars.json --replace       # preview replacements
ck agents avatars --manifest avatars.json --replace --yes # replace, one Touch ID
```

Seed-only is the default: existing avatars are listed as skipped, so the same
manifest can also give newly created agents their first creature. An all-skipped
run succeeds without a prompt or a journal entry. `--replace` overwrites every
listed agent but refuses a genome exactly equal to that agent's current genome.
Only full equality is refused; a wild genome may begin with its classic genome.

The preview lists names, old and new types/versions, and skipped agents. The
module validates the whole manifest before any prompt, refusing unknown fields,
non-arrays, empty arrays, duplicate ids, non-live agents, unsupported types,
wrong genome lengths, and entries that disagree on type. It reports every domain
problem together, and writes nothing on a refusal. `--yes` requests one Touch ID
for all changes, with a count/type summary bounded to 200 characters (never
truncated). The summary also binds the complete plan: identities or avatars
changing while approval is pending refuse `operator_approval_stale`.

Before the first write, the writer checks the complete approved plan under its
lock. Each changed agent then gets its own normal `agent.set_avatar` transaction
and shared append, using the key `<batch key>:<agent_id>`. This avoids an oversized
shared entry and remains compatible with older replicas. A failure can leave a
partially written batch: the reply names exactly which agents were written and
which remain. Retry with the original `--request-key` and unchanged manifest:
already written entries replay their cached replies, including identical genomes,
and the remaining entries apply after a new batch confirmation. A completed
batch replays without another prompt.
Only the operator can apply manifests; flows and other modules are refused.

Verify: run the preview again (seed-only reports all entries skipped). Reverse:
apply a saved manifest of the old genomes with `--replace --yes`.

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

Verify: `ck agents repair-workspaces` (the preview) reports nothing to change.

Reverse: none as a single command. The change is journaled; rename or move
agents individually if needed.

## Where an agent is

```sh
ck agents where Ada-2              # from the local claim cache
ck agents where Ada-2 --refresh    # refresh claims from engram first
```

The answer says whether the claim is known and which Mac holds it. Reads never
wait for engram unless you pass `--refresh`.

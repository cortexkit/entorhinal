# What "operator approval" can mean for agent identity

Written 2026-10-03 by the entorhinal maintainer, for the project owner, who
decides it. It answers the one question left open in
`prefrontal/docs/designs/agent-identity-entorhinal.md`.

Every claim below comes from source read on that date, or from the deployed
stores read read-only, and names the file and line or the query it came from.
Figures describing the running fleet are a snapshot of one deployment on one
machine, not a general property of the design.

## The question

Agent identity moves from prefrontal-core into entorhinal. Entorhinal's write
gate admits a mutation from a `Direct` principal (any local process running as
the operator, including host plugins, not only `ck`) or from
`reserved:prefrontal-core`; every other principal is refused
`write_not_permitted` (`entorhinal/crates/entorhinal-module/src/main.rs:182-230`).
The code states it is not a security boundary.

So: when the spec says agent identity needs "operator approval", does that mean
any local process running as the operator, or something narrower?

## Correction to the draft's premise

The draft says entorhinal "already holds the operator-approval pattern (it mints
confirmation codes under its own authority for project roots)"
(`agent-identity-entorhinal.md:9`). **It does not.** There is no confirmation
code, challenge or nonce anywhere in `entorhinal-core`.

Project-root approval is a stored boolean: `root_approval` keyed by the root's
registration epoch (`entorhinal/crates/entorhinal-core/src/binding.rs:59-62`),
set by `approve_root` and `unapprove_root`, each of which journals an op carrying
an `actor` string (`binding.rs:734-780`, `binding.rs:588-599`). Both pass through
the same write gate as every other mutation, and neither asks the caller to prove
anything beyond being admitted by it. `ck projects approve` is simply one such
`Direct` caller.

Consequence: **"operator approval" in entorhinal today already means "a local
process running as the operator said so".** There is no proof-of-human primitive
in the module to reuse. Any option that needs one needs new code.

## How often agent identity actually changes

Read read-only from the deployed core store
(`~/.local/share/cortexkit/prefrontal-core/store.db`) on 2026-10-03. Each row
counts rows in the `agent` table as they stood then, not events over time, except
where it says otherwise.

| fact | value |
| --- | --- |
| agents in the store | 35 |
| of those, created on 2026-08-13 by the one-time fleet seed | 29 |
| of those, created individually after that seed | 6 (08-16, 09-28, 09-29, 09-30 x2, 10-03) |
| carrying a terminal disposal reason | 0 |
| merged into another agent | 0 |
| name claims ever released, which is what a rename does | 0 of 35 claims |
| carrying a github identity | 25 |
| carrying an avatar | 30 |
| carrying labels | 35 |

So after the one-time seed, creating an agent is a deliberate act at roughly one
a week, and the three destructive ops — dispose, merge and rename — have **never
run in production at all**.

The last three rows are state, not frequency: the schema keeps no per-field
timestamp, so the store cannot say when an avatar, label set or github identity
was written, or how often it was rewritten
(`prefrontal/crates/prefrontal-core-store/migrations/076_agent_registry.sql:1-23`).
What is known about those is their callers, below.

## Can anything mint an agent id without a human?

Not in this deployment. The only automated path is the legacy boot recovery
`recover_pending` (`prefrontal/crates/prefrontal-core-module/src/cutover_ceremony.rs:39-66`),
which can insert rows via `apply_ceremony_seed_and_activate`. It returns
`Ok(false)` immediately when the registry activation marker is set
(`cutover_ceremony.rs:46-48`), and the live store has
`agent_registry_activation_marker` row `1` stamped `1786643952460`. The path is
permanently closed here.

No scheduler, reconciler, wake handler, campaign, flow or worker launch creates
an agent. The production callers of the mutating ops are operator-run scripts:
`script/avatar-seed.ts` for avatars and `script/github-app-batch.ts` for GitHub
identity, both after a human ceremony.

**So a confirmation gate on identity mutation would deadlock nothing.**

## Why the destructive ops deserve more than the others

An agent id is not just a row. It carries authority held by other modules:
wernicke posts as the agent's GitHub bot, plexus checks per-agent grants, basal
flows act as their owner agent. The draft settles that grants stay with each
provider, keyed by the entorhinal agent id, and that retirement fans out as a
notice each provider acts on (`agent-identity-entorhinal.md:38`).

Two consequences follow from that fan-out. Retiring or merging an agent is
irreversible across module boundaries, because each provider acts on the notice
by purging its own rows and entorhinal cannot un-purge them. And a re-used id
would inherit whatever authority the previous holder still had with those
providers, which is why the draft requires agent ids to be random and permanently
tombstoned rather than derived like project ids
(`agent-identity-entorhinal.md:33`).

Changing a label, avatar or tag triggers no notice, purges nothing in another
module, and can be undone by setting the old value back.

## The three options

**A. Keep the gate as it is.** Any local process running as the operator may
mint, rename, retire and merge agents. No new code, and consistent with how
project-root approval already behaves. Accepts that "operator approval" means
"a local process running as me", including any buggy script or plugin.

**B. Route agent mutations through prefrontal-core only.** Entorhinal would
refuse `Direct` for agent ops and admit only `reserved:prefrontal-core`. This
adds a choke point for policy and audit, but core itself admits `Direct` callers
(`prefrontal/crates/prefrontal-core-module/src/main.rs:354-390`), so it moves the
boundary without creating one. It also cuts against the move's own stated goal:
running without prefrontal, or with a different orchestrator, must keep agent
identity (`agent-identity-entorhinal.md:8`).

**C. Build a real confirmation gate for the destructive subset.** Mint, rename,
retire, merge and GitHub identity require an out-of-band operator confirmation
that entorhinal issues and verifies; labels, avatar and tag stay on the plain
gate. This is the only option that is actually a gate rather than a statement
about which process asked. It is new code in entorhinal.

How often it would interrupt anyone is partly measurable and partly not. Of the
five gated ops, four have a measurable history: six creations since the seed, and
zero renames, retires or merges ever. GitHub identity is the unmeasurable one —
25 agents carry one, but the schema records no timestamp, so the store cannot say
when or how often it was written. Its only production caller is an operator-run
batch script invoked after a human GitHub ceremony (`script/github-app-batch.ts`),
which is a human-paced act rather than an automated one.

## Recommendation

**C, scoped to mint, rename, retire, merge and GitHub identity.**

The gate's value is highest exactly where the action is irreversible and fans out
into other modules' grants, and its cost is lowest there too, because those ops
are rare and deliberate. A is cheap, but it leaves the most destructive
fleet-wide action — retiring an id that other modules have granted authority to —
reachable by any script running as the operator. B buys indirection at the price
of the replaceability the move exists to achieve.

The honest argument against C is that it is the only option that adds a
primitive, and a gate nobody can satisfy when the operator is away is a liveness
risk. That argues for scoping it tightly, not for skipping it: a fleet that
cannot retire an agent until its operator confirms is in a safe state, while one
that retires an agent because a script misfired is not.

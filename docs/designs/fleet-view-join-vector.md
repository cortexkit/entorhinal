# Fleet view join vector

Draft for review by the clients that will implement it. Written 2026-10-03 by
the entorhinal maintainer, from the shape `agent.fleet_overview` serves today.

`agent.fleet_overview` returns one row per agent joining identity, residence,
board status and pending asks. When agent identity moves to entorhinal, no
module can answer that from its own state, so the op is deleted and **the client
composes two calls**. This document is the contract that stops three clients
(phone, TUI, desktop) from each getting that join subtly different.

It is read-path only; nothing here depends on the write-gate decision.

## What each half owns

Partitioned from `FleetAgentRow` (`prefrontal/crates/prefrontal-core-module/src/fleet_overview.rs:49-84`)
so that the join reproduces today's reply field for field. Nothing is dropped,
and nothing changes name.

**Entorhinal's half** — identity, and the project registry it already owns:

| field | notes |
| --- | --- |
| `agentId` | the join key |
| `displayName` | optional |
| `tag` | optional |
| `labels` | array, never absent; empty is `[]` |
| `role` | string |
| `avatar` | `{fingerprint, type?, version?}`, optional |
| `project` | `{id, name, canonicalRoot?}`, optional |
| `workspace` | `{id, name}`, optional |

Entorhinal can serve all of it without calling anyone: it will hold agent
identity, and it already holds projects, workspaces and roots. That is what
makes this half self-contained, where core's version of it is not — core builds
`project` and `workspace` today by calling `projects.enumerate`
(`fleet_overview.rs:169-252`).

**Core's half** — session and behaviour:

| field | notes |
| --- | --- |
| `agentId` | the join key; core keys its half by this, never by session |
| `residence` | `{harness, state, session}`, optional as a whole; see below |
| `board` | `{statusText?, statusState?, updatedAtMs?}`, optional |
| `pendingAskCount` | **attributed by agent**, see below; absent is not zero |
| `latestActivityMs` | optional |

`pendingAskCount` changes meaning in this move, deliberately. Today core
attributes asks **by project roots**: with a project that has roots it sums
`asks.count_by_directory` over them, falling back to `count_by_session` only
when there are no roots, and leaving the field absent only when neither applies
(`fleet_overview.rs:387-407`). After the move core does not own project roots,
so keeping that rule would mean asking Entorhinal on a request path — the
coupling this whole design removes.

So **core counts by agent**: the asking session's agent, or `headAgentID` where
the ask carries one, keyed by `agentId`. The field is then computed for every
agent core knows and absent only when attribution genuinely fails.

That is also a correction, not only an accommodation. Counting by directory
counts asks from *any* session working in that project, so today's per-agent
column can show asks that belong to a different agent. "Asks attributable to
this agent" is what a per-agent field should mean, and it is what clients will
get.

`residence.session` is optional, and **`residence` present with no session means
"running somewhere this client cannot open"** — the agent is live, but there is
no session to attach chat or ask attribution to. The dock shows it as running,
without chat and without attributed asks.

This is a live latent bug, not a hypothetical. `session` comes from
`residence_session`, which returns `None` whenever the stored `address_json`
fails to parse, has no `session` key, or has an empty one
(`fleet_overview.rs:458-465`), and the row is built with whatever that returns
(`:381-385`). The phone's decoder currently requires the field, so one such row
fails the whole decode **today**. CKIOS is making the phone tolerate it.

An earlier draft of this document proposed that core omit `residence` entirely
when it has no session. That was wrong and is recorded so it is not re-proposed:
omitting it would render a running agent as `notRunning`, which is a worse
answer than an incomplete one.

The observed `state` values are `active` and `sleeping`, from
`row.sleep` (`fleet_overview.rs:383`), not `live`. It stays an opaque string so
a producer adding a state cannot break decoding, but the vector pins the
observed set from reply bytes rather than from memory.

`harness` is one of `opencode`, `broca`, `pi` (`:476-482`); `role` is one of
`assistant`, `workspace_head`, `head`, `hiree` (`:467-474`). Both are pinned as
observed sets and read as opaque strings.

Core must key by `agentId` even though pending asks carry `askerSessionID`
rather than an agent id. If core keyed its half by session, every client would
re-derive the agent-to-session map and a residence move would orphan asks.

## The avatar fingerprint moves verbatim

`avatar.fingerprint` is a cross-implementation contract, not an implementation
detail: lowercase hex of the first 16 bytes (32 hex chars) of BLAKE3 over the
UTF-8 concatenation `genome-hex + '\n' + type-or-empty + '\n' +
decimal-version-or-empty`, with the delimiters present precisely so field
boundaries cannot alias (`fleet_overview.rs:96-109`).

It is computed at serve time from the stored triple; there is deliberately no
stored fingerprint column, because a stored copy could drift from its inputs.
Entorhinal reimplements the same function over the same triple, and a vector
case below pins a known genome to a known fingerprint so the two
implementations cannot diverge silently. The hazard this protects against is
specific: an avatar re-roll keeps the agent id, so a client caching genomes by
`agentId` would serve the previous creature forever with no failure anywhere.

## Caching rule

Clients key avatar genome and raster caches on `avatar.fingerprint` only, never
on `agentId`. An avatar re-roll keeps the agent id, so a cache keyed by id
serves the previous creature forever with no failure anywhere — no error, no
stale indicator, nothing to notice. This is a contract rather than advice,
because the failure is silent and permanent.

## The join

Identity is the left side, always. Entorhinal is the authority for which agents
exist.

1. **Identity row with a core row carrying residence** → `running`.
2. **Identity row with a core row whose residence is absent** → `notRunning`.
   Core returns the row with residence absent rather than omitting the row, so
   the two halves never disagree about the fleet's membership.
3. **Identity row with no core row at all** → `unknown`. This is an error-free
   state and must be distinct from (2): core's set can legitimately lag
   entorhinal's for a just-registered agent, and a client cannot tell "not
   running" from "core has not caught up" unless the two are separated.
4. **Core row with no identity row** → dropped. Identity is the authority.
5. **Duplicate `agentId` in either half** → the half is in error. Both sides
   refuse duplicates by contract. A client **renders exactly one row, keeping
   the first, and reports the duplicate where the operator can see it** — for
   the phone, the Connection screen's diagnostics. It does not replace the view
   with an error: a dock keyed by agent id must render one row or its lookups
   break. The requirement is that the condition is reported somewhere a human
   will find it, not that the view fails.

Ordering is not part of the contract. Clients sort by their own criteria.

`running` has no sub-states in the join. The detail a client renders comes from
fields core already sends: `residence.state` (kept as an opaque string, so a
producer adding a state cannot break decoding), `board.statusState` (drives
attention order and avatar mood), and `latestActivityMs` (staleness is derived
from age on the client, never sent).

**Any count the join cannot compute is absent, never 0.** `pendingAskCount`
first, because "cannot attribute" must never read as "has none" — but as a
general rule, so the next field added does not rediscover it.

## Partial failure

Each half fails independently and neither failure is fatal to the view.

- **Identity call fails, with a previous set** → keep the previous identity set.
  Identity changes rarely, so a stale identity set is nearly always correct.
- **Identity call fails, with no previous set** → "could not load the fleet".
  This must be distinct from an empty fleet. A client must never draw "you have
  no agents" when the truth is "could not ask". The case is reachable on a first
  launch, and on any new machine or account, since a client's fleet cache is
  scoped per machine and account.
- **Core call fails** → **keep each row's last known core fields.** Only a row
  with no prior core data at all renders `unknown`. A failed poll must not flip
  populated rows to `unknown`, or the view flickers on every network blip; this
  is what the phone already does, and the contract follows it rather than
  overriding it.
- **Both fail** → keep the last composed view.

So `unknown` means "core has never told us about this agent", never "the last
poll failed". An `unknown` that never resolves is the only wrong outcome here.
If anything bounds how long it can last — core picking up a newly registered
agent on its next registry read, say — this document states that bound as a
measured number rather than a promise. **Open: nothing measured yet.**

## Unchanged token

Entorhinal's half supports an unchanged token in the shape
`session.transcript_page` already uses. A client sends the token it last
received; if nothing has changed, the reply says so and carries no rows.

This is a condition of the design rather than an optimisation. Identity barely
changes, so with the token a client polls only core's half at its usual cadence
and asks entorhinal rarely, which makes the split **cheaper** than today's
single fat reply. Without it, the split is strictly worse than what clients have
now, and the right answer would be a different design.

## Vector cases

Each case is identity reply, core reply, expected joined rows. A client pins
these; entorhinal and core pin the halves they own.

1. **Both halves complete** — one agent, residence present, board present,
   `pendingAskCount: 2`. Expect `running` with every field populated.
2. **Identity without residence** — core returns the row, `residence` absent.
   Expect `notRunning`, identity intact, `board` absent, and `pendingAskCount`
   **a real count** whenever asks are attributable to the agent, since the new
   rule attributes by agent rather than by session: an agent with no residence
   can still own pending asks. Absent only when attribution fails. This case is
   live today: such an agent is listed, filters and labels apply, it has no
   chat, no board status, and consent cards naming it go to the fleet-level
   list. It must render explicitly and never vanish — the registry knows it and
   the operator may want to wake it.
3. **Identity with no core row** — core's reply omits the agent entirely. Expect
   `unknown`, identity intact, no behaviour fields.
4. **Core row with no identity** — expect the row dropped, and nothing rendered.
5. **Duplicate `agentId` from core** — expect an error surfaced, not a silent
   first-wins. The phone has hit this live: one agent under two residences, and
   a row repeated mid-change.
6. **Duplicate `agentId` from entorhinal** — same. Note that entorhinal has a
   structural reason to be safe here, since one live head per project is
   schema-enforced, but "the schema makes it unlikely" is not a contract.
7. **Core call fails, rows already populated** — expect each row to keep its
   last known core fields, and no row to change state. Nothing becomes
   `unknown`.
8. **Core call fails, a row never populated** — expect that row alone at
   `unknown`, with populated rows unaffected.
9. **Identity call fails, previous set held** — expect the previous identity set
   retained and core's fields applied to it.
10. **Identity call fails, no previous set** — expect "could not load the
    fleet", and specifically **not** the empty-fleet rendering of case 13.
11. **Avatar fingerprint** — a fixed genome, type and version pinned to a fixed
    32-char fingerprint, plus one case with type and version absent, so the
    empty-string contribution and the delimiters are both exercised.
12. **Unchanged token** — three parts. A second identity call with the previous
    token and nothing changed returns unchanged and no rows. A second call
    after an **avatar re-roll** returns rows, not unchanged. A second call after
    a **label change** returns rows. Any identity change invalidates the token;
    a token that survives a re-roll would freeze every client's avatar cache on
    the previous creature, which is the same silent failure the fingerprint
    caching rule exists to prevent.
13. **Empty fleet** — both halves succeed and are empty. Expect an empty view,
    not an error, and distinguishable from case 10.
14. **Residence without a session** — reachable today (`residence_session`
    returns `None` on an unparseable, missing or empty `session` in
    `address_json`). Expect the row to decode with `{harness, state}` alone and
    render as **running**, without chat and without attributed asks. Not
    `notRunning`: the agent is live. The case exists because a decoder that
    required `session` failed the entire reply on one such row.

## Open

- The bound on `unknown`, if any, as a measured number.
- Whether `running` needs a sub-state the current reply does not carry. Only the
  clients can answer; the field list above is derived from the server's reply,
  not from what a screen needs.
- The phone cannot reach entorhinal today: every phone call goes to
  prefrontal-core. Callosum must expose entorhinal to phones and the phone's
  read must be granted in the same change. This is a prerequisite of the whole
  design, not a detail of the join.

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
| `residence` | `{harness, state, session?}`, optional |
| `board` | `{statusText?, statusState?, updatedAtMs?}`, optional |
| `pendingAskCount` | optional, and **absent is not zero** |
| `latestActivityMs` | optional |

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
   refuse duplicates by contract; a client must surface the condition rather
   than silently keep the first row, because a client that silently picks is a
   client that cannot tell anyone the registry is wrong.

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

- **Identity call fails** → the client keeps its previous identity set. Identity
  changes rarely, so a stale identity set is nearly always correct.
- **Core call fails** → every row renders at `unknown`, exactly as if core
  returned nothing. This is why `unknown` must be a neutral state with no
  warning colour and no timeout that promotes it to "broken".
- **Both fail** → the client keeps its last composed view.

An `unknown` that never resolves is the only wrong outcome here. If anything
bounds how long it can last — core picking up a newly registered agent on its
next registry read, say — this document states that bound as a measured number
rather than a promise. **Open: nothing measured yet.**

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
   Expect `notRunning`, identity intact, `board` absent, `pendingAskCount`
   **absent** (not 0). This case is live today: such an agent is listed, filters
   and labels apply, it has no chat, no board status and no session-attributed
   asks, and consent cards naming it go to the fleet-level list. It must render
   explicitly and never vanish — the registry knows it and the operator may want
   to wake it.
3. **Identity with no core row** — core's reply omits the agent entirely. Expect
   `unknown`, identity intact, no behaviour fields.
4. **Core row with no identity** — expect the row dropped, and nothing rendered.
5. **Duplicate `agentId` from core** — expect an error surfaced, not a silent
   first-wins. The phone has hit this live: one agent under two residences, and
   a row repeated mid-change.
6. **Duplicate `agentId` from entorhinal** — same. Note that entorhinal has a
   structural reason to be safe here, since one live head per project is
   schema-enforced, but "the schema makes it unlikely" is not a contract.
7. **Core call fails entirely** — expect every identity row at `unknown`.
8. **Identity call fails entirely** — expect the previous identity set retained
   and core's fields applied to it.
9. **Avatar fingerprint** — a fixed genome, type and version pinned to a fixed
   32-char fingerprint, plus one case with type and version absent, so the
   empty-string contribution and the delimiters are both exercised.
10. **Unchanged token** — a second identity call with the previous token returns
    unchanged and no rows; the client's view is unaffected.
11. **Empty fleet** — both halves empty. Expect an empty view, not an error.

## Open

- The bound on `unknown`, if any, as a measured number.
- Whether `running` needs a sub-state the current reply does not carry. Only the
  clients can answer; the field list above is derived from the server's reply,
  not from what a screen needs.
- The phone cannot reach entorhinal today: every phone call goes to
  prefrontal-core. Callosum must expose entorhinal to phones and the phone's
  read must be granted in the same change. This is a prerequisite of the whole
  design, not a detail of the join.

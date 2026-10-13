# `agent-data/v1`: the participant contract for agent moves

Status: draft for review by SUBC (capability owner) and the implementers (MC, BROCA, OC, PI, THALAMUS). Nothing is built against it yet. Its home will be a commons role crate, `cortexkit-role-agent-data` (CONTRACT.md, typed structs, test vectors), which SUBC publishes; this note is the draft it starts from.

An agent's data lives in several modules. When entorhinal moves an agent from one Mac to another, it asks each of those modules, the participants, to stop the agent's data at a cut, ship it through engram's session door, and bring it up on the other side. This note pins the wire contract entorhinal calls. The move protocol itself (the move record and its states) is in entorhinal's part 2 spec. The door ops (`cut.*`, `session.*`) are engram's.

## Who and how

- A participant declares `agent-data/v1` in `capabilities.provides`. Entorhinal finds participants through the daemon catalog.
- Every op that changes state is accepted only from `reserved:entorhinal`, decided on the route's daemon-stamped principal and never on anything in the request body. Any other caller (sessions, flows, other modules) is refused `caller_forbidden`. The reads `describe`, `busy` and `status` are also open to `direct`, so the operator can diagnose with `ck`.
- Callosum never re-exports `agent-data/v1`: a move never reaches another Mac through the router, only through engram's door.
- Requests and replies are JSON objects. Top-level fields are strict (an unknown one is `invalid_request`); nested payloads are tolerant, like the other role contracts.
- A request carries at most 256 refs, so every reply stays well under the daemon's 4 MiB frame (3 MiB is the working ceiling). Data bytes never travel in these messages: they go through the door.
- A **ref** is `{ref_kind, ref}`: an opaque handle the participant uses for one piece of an agent's data. It's the same pair the orchestrator or host registered with `agent.register_session`. Entorhinal sends each ref only to the participant whose `describe` declares its `ref_kind`, and a participant refuses a ref it doesn't hold with `unknown_ref`, naming the ref.
- A **`cutId`** is 16 random bytes in lowercase hex, minted by entorhinal per move. Every op except `describe` and `busy` carries one.
- **Idempotency:** every op that changes state is idempotent on `(cutId, op, refs)`. A repeat returns the first outcome. The same `cutId` and op with different refs is refused `id_reused`.
- **Unknown is not refused:** a transport failure or timeout means the outcome is unknown, never that the op didn't happen. The driver resolves it with `status`, then repeats the op if needed, which idempotency makes safe.
- **Order:** a participant keeps per-`cutId` state and refuses an op that skips a step with `invalid_state`, naming its current step. The steps are below.

## Refusal codes

| Code | Meaning | Retry? |
| --- | --- | --- |
| `caller_forbidden` | Not `reserved:entorhinal`. | No |
| `invalid_request` | Malformed request or unknown field. | No |
| `unknown_ref` | This participant doesn't hold that ref. | No |
| `id_reused` | Same `cutId` and op with different refs. | No |
| `invalid_state` | Op out of order for this `cutId`. `detail.step` names the current step. | No |
| `cut_unknown` | No state for this `cutId` (for example, `export` before `prepare`). | No |
| `busy` | The data can't be cut now. `detail.refs` lists which refs, each with a reason. | Yes, later |
| `not_exportable` | This data must never leave this Mac (for example, broca sessions with caller-chosen retention). | No: the move is refused. |
| `schema_behind` | This participant's store is on an older schema than the cut needs. `detail` names both versions. Refused on `import` before anything is staged. | No: upgrade that Mac first. |
| `digest_mismatch` | Imported bytes don't hash to the cut's digest. | No: the move aborts. |
| `unavailable` | Temporary failure. | Yes |

A participant can't invent other codes; anything outside this set is treated as `unavailable`.

## Ops

### `agent_data.describe {}`
Reply: `{ref_kinds: [string], stores: [{name, sync_class}], external: bool}`.
- `ref_kinds`: the ref kinds this participant holds.
- `stores`: its per-agent stores and their sync classes, as AGENT_SYNC defines them.
- `external`: true when part of the agent's data is a file another program owns and ships itself (for example, THALAMUS's harness: the OpenCode database or Pi and Claude Code JSONL transcripts). The participant still cuts its own state; `prepare` names the external part.

### `agent_data.busy {refs}`
Reply: `{refs: [{state, reason?}]}`, in request order. `state` is `idle`, `busy` or `unknown`.
- `busy` covers anything that would change the data during a move: a turn or run in flight, a queued send, an open approval card.
- `unknown` counts as busy.
- `reason` is short text for the operator.
A read: it changes nothing and needs no `cutId`.

### `agent_data.status {refs, cutId}`
Reply: `{step}`, one of `none`, `prepared`, `exported`, `imported`, `activated`, `aborted`, `released`. A read, used by the driver after an unknown outcome.

### `agent_data.prepare {refs, cutId}`
The source decides where the data ends and freezes it where it can. Reply:
```
{items: [{ref_kind, ref, sessionRef, stream, endOffset, digest, frozen}],
 external: [{ref_kind, ref, note}]}
```
- `sessionRef`, `stream`, `endOffset` and `digest` go into `cut.seal` unchanged. `stream` is this participant's principal (`reserved:<module_id>`). `digest` is SHA-256 over the stream's bytes `[0, endOffset)`, lowercase hex, matching the door.
- Before replying, the participant checks with `session.cover` that everything it admitted so far is covered.
- `frozen: false` means the participant can't stop writes. Anything written after `endOffset` stays on this Mac and becomes a fork if the agent is used here again.
- `external` lists data that travels outside the door, with a note for the operator (for example, "OpenCode session database: copy it with the project").
- Refusals: `busy` (a busy check is repeated here, atomically with the freeze), `not_exportable`.
- Step: none → `prepared`.

### `agent_data.export {refs, cutId}`
The source makes sure every item's bytes up to `endOffset` are admitted through the session door (`session.admit` on engram's `session-door` service). Reply: `{items: [{sessionRef, endOffset}]}` once all are admitted. Entorhinal then calls `cut.seal`.
- Step: `prepared` → `exported`.

### `agent_data.diverged {refs, cutId}`
On the receiver, before import: compare this Mac's copy with the cut. Reply: `{refs: [{state}]}`, `state` being `at_cut` (this Mac's copy is an exact prefix of the cut, or absent), `advanced` (this Mac has data past where the cut starts that the cut doesn't contain) or `unknown`.
- `advanced` anywhere stops the move and shows the operator the fork choice. `unknown` blocks too and is never read as `at_cut`.
- A read: no step change.

### `agent_data.import {refs, cutId}`
On the receiver: read the cut through the door (`cut.read`, then `session.read` with the `cutId`, which needs no lease) into staging, and check every digest. Reply: `{items: [{sessionRef, endOffset, digest}]}`.
- If this Mac holds a released (tombstoned) copy of the same refs, which happens on a move back, the import replaces the tombstone; that is the expected case, not an error.
- Refusals: `digest_mismatch`, `cut_unknown` (the cut isn't readable yet), `schema_behind`.
- Step: none → `imported`.

### `agent_data.activate {refs, cutId}`
On the receiver: make the staged data live. Reply: `{}`.
- Step: `imported` → `activated`.

### `agent_data.abort {refs, cutId}`
On either side, only before the move record reads `activated`: the source unfreezes, the receiver drops its staging. Reply: `{}`. Aborting a `cutId` this participant never saw succeeds and does nothing.
- Refused `invalid_state` after `activated` (receiver) or `released` (source).
- Step: any earlier step → `aborted`.

### `agent_data.release {refs, cutId, holder}`
On the source, after the move record reads `activated`: the freeze becomes a tombstone that names the new holder (`holder` is its author pseudonym). Later local writes for these refs are refused, telling the user the agent now lives on another Mac. Reply: `{}`.
- Step: `exported` → `released`.

## A move back

The original Mac is just another receiver: `diverged`, then `import` and `activate` on the incoming cut. Entorhinal calls `cut.accept` first, so the participant can write to its door containers again. There is no separate resume op. A participant whose data is an append-only log (broca's WAL) accepts the incoming cut only if it extends the local copy byte for byte; otherwise `diverged` answers `advanced`.

## Open for review

- Sync class names in `describe`: take AGENT_SYNC's list as-is, or pin the strings here.
- Whether any participant needs `prepare` to return more than one stream per ref.
- Whether `busy.reason` needs a code alongside the text, for the phone.

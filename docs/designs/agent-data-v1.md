# `agent-data/v1`: the participant contract for agent moves

Status: second draft, with the crate review (agent-sync posts 396-415) folded in. SUBC, BROCA, THALAMUS and MC have approved it; OC and PI have not reviewed yet. The binding version will be a commons role crate, `cortexkit-role-agent-data` (CONTRACT.md, typed structs, test vectors), which SUBC builds and publishes from this note. Implementers pin the crate, not this note.

An agent's data lives in several modules. When entorhinal moves an agent from one Mac to another, it asks each of those modules, the participants, to stop the agent's data at a cut, ship it through engram's session door, and bring it up on the other side. This note pins the wire contract entorhinal calls. The move protocol itself (the move record and its states) is in entorhinal's part 2 spec. The door ops (`cut.*`, `session.*`) are engram's.

## Who and how

- **Declaring it:** a participant declares `agent-data/v1` in `capabilities.provides`. Entorhinal finds participants through the daemon catalog.
- **Admission:** the caller is the principal the daemon stamps on the route bind, never anything in the request body. Every op is accepted only from `reserved:entorhinal`; any other caller (sessions, flows, `direct`, other modules) is refused `caller_forbidden`.
- **No Callosum:** Callosum never re-exports `agent-data/v1`. A move never reaches another Mac through the router, only through engram's door.
- **Strictness:** request and reply objects are JSON. Top-level fields, and every sub-object this contract types, are strict: an unknown field is `invalid_request`. Values a participant passes through opaquely (`describe.detail`, `prepare.items[].format`, `prepare.external[].check`) are tolerant, so a newer participant can add to them without breaking an older entorhinal.
- **Size:** a request carries at most 256 refs, and each ref yields at most one stream, so every reply stays well under 3 MiB per frame (the daemon cuts a connection at 4 MiB). Data bytes never travel in these messages: they go through the door. `describe` lists are bounded by a participant's own ref kinds and stores.

### Refs and the session handle

- **A ref** is `{ref_kind, ref}`: an opaque handle for one piece of an agent's data, the same pair the orchestrator or host registered with `agent.register_session`. Entorhinal sends each ref only to the participant whose `describe` declares its `ref_kind`.
- **On the source**, a participant refuses a ref it doesn't hold with `unknown_ref`, for the whole request, naming the ref.
- **On the receiver**, `diverged` and `import` accept refs the participant doesn't hold yet: that's a first-time import. For such a ref `diverged` answers `at_cut`.
- **`sessionRef`** is exactly `cortexkit_role_llm_runner::session_ref` (commons 8ebc492, llm-runner 0.2.1), the 16-byte hash participants already share. Nobody derives their own.

### `cutId`, idempotency and lost replies

- **`cutId`:** 16 random bytes in lowercase hex, minted by entorhinal per move. Every op except `describe` and `busy` carries one.
- **Idempotency:** every op that changes state (`prepare`, `export`, `import`, `activate`, `abort`, `release`) is keyed by `(cutId, op)`, and the whole request is compared, with `refs` compared as a set. A repeat with the same fields returns the first reply. A repeat with any field different (the ref set, `holder`) is refused `id_reused`. Only successful replies are recorded, so a refusal (`busy`, `cut_not_ready`) is decided again on retry. The reads `describe`, `busy`, `status` and `diverged` are exempt.
- **Ref sets:** a request's refs are distinct; a duplicate is `invalid_request`. An empty list is allowed. Every op after the first for a `cutId` must name exactly the ref set that `cutId` started with: `export`, `release` and the source's `abort` against `prepare`, and `activate` and the receiver's `abort` against `import`. A mismatch is `invalid_state`, since it's checked against stored per-cut state, with the expected set in `detail`. Otherwise part of an agent could stay staged on the receiver, or writable on the source, after the move.
- **Lost replies:** a transport failure, timeout or connection drop means the outcome is unknown, never "not done". The driver resolves it with `status`, or by sending the same op again, which idempotency makes safe. A participant makes every op safe to repeat after a crash between doing it and replying.
- **Durability:** the freeze `prepare` makes and the tombstone `release` makes are durable before those ops reply, so a participant restart can't let a write land past the cut.

## Refusal codes

| Code | Meaning | Driver |
| --- | --- | --- |
| `caller_forbidden` | Not `reserved:entorhinal`. | Abort |
| `invalid_request` | Malformed request or unknown typed field. | Abort |
| `unknown_ref` | Source side: this participant doesn't hold that ref. | Abort |
| `id_reused` | Same `(cutId, op)` with a different request. | Abort |
| `invalid_state` | Op out of order for this `cutId`. `detail.step` names the current step. | Abort |
| `cut_unknown` | No state for this `cutId` here (for example, `export` before `prepare`). | Abort |
| `cut_not_ready` | The cut isn't readable through the door yet. | Retry within the step |
| `busy` | The data can't be cut now. `detail.refs` lists the refs with a reason each. | Retry later |
| `not_exportable` | This data must never leave this Mac (for example, broca sessions with caller-chosen retention). | Abort; the move is refused |
| `incompatible_version` | This participant's store can't accept this cut. `detail.found` and `detail.required` give both versions. | Abort; upgrade that Mac first |
| `digest_mismatch` | Imported bytes don't hash to the cut's digest. | Abort |
| `external_not_present` | The host's own data for a ref isn't on this Mac yet, or doesn't match the cut's check. `detail.note` repeats the note. | Retry after the operator copies it |
| `unavailable` | Temporary failure. | Retry |

**Unknown codes:** a code outside this set is never retried. The driver aborts the move and shows the code.

## Ops

### `agent_data.describe {}`
Reply: `{ref_kinds: [string], stores: [{name, sync_class}], external: bool, detail?}`.
- `ref_kinds`: the ref kinds this participant holds.
- `stores`: its per-agent stores, with `sync_class` taken verbatim from AGENT_SYNC's list.
- `external`: true when part of an agent's data is a file another program owns and ships itself (for example, Claude Code transcripts behind THALAMUS's harness). A participant that ships all of its data through the door, as the OpenCode adapter does with its row log, declares `false`.
- `detail`: optional, opaque, tolerant.
A read.

### `agent_data.busy {refs}`
Reply: `{refs: [{state, reason_code?, reason?}]}`, in request order. `state` is `idle`, `busy` or `unknown`.
- `busy` covers anything that would change the data during a move: a turn or run in flight, a queued send, an open approval card, a background job.
- `unknown` counts as busy.
- `reason_code` is a short code the phone can show without parsing text, for example `turn_in_flight`, `run_active`, `send_queued`, `approval_open`, `historian_running`, `dreamer_running`. `reason` is short operator text.
A read.

### `agent_data.status {refs, cutId}`
Reply: `{step}`, one of `none`, `prepared`, `exported`, `imported`, `activated`, `aborted`, `released`. A read, used by the driver after an unknown outcome.

### `agent_data.prepare {refs, cutId}`
On the source: decide where the data ends, and freeze it where possible. Reply:
```
{items: [{ref_kind, ref, sessionRef, stream, endOffset, digest, format, frozen}],
 external: [{ref_kind, ref, note, check}]}
```
- **Fields that feed the seal:** `sessionRef`, `stream`, `endOffset` and `digest` go into `cut.seal` unchanged.
  - `stream` is this participant's principal (`reserved:<module_id>`).
  - `endOffset` is a length in bytes of that stream.
  - `digest` is SHA-256 over the stream's bytes `[0, endOffset)`, lowercase hex, matching the door.
- **`format`:** opaque text naming the data's store version. The receiver's `import` checks it before reading, and refuses `incompatible_version` if it can't accept it.
- **`frozen`:** `frozen: false` means the participant can't stop writes. Anything written after `endOffset` stays on this Mac and becomes a fork if the agent is used here again.
- **`external`:** lists data the host program ships itself.
  - `note` is the operator text (for example, "Claude Code transcript: copy it with the project").
  - `check` is opaque to entorhinal: whatever the receiving participant needs to recognise the right host data (for THALAMUS, the host's native session id, the last host row id, and a digest of the transcript up to it).
- **Before replying:** the participant checks with `session.cover` that everything it admitted so far is covered, and makes the freeze durable.
- **Refusals:** `busy` (checked again here, atomically with the freeze), `not_exportable`.
- **Step:** none → `prepared`.

### `agent_data.export {refs, cutId}`
On the source: make sure every item's bytes up to `endOffset` are admitted through the door (`session.admit` on engram's `session-door` service). Reply: `{items: [{sessionRef, endOffset}]}` once all are admitted. Entorhinal then calls `cut.seal`.
- Step: `prepared` → `exported`.

### `agent_data.diverged {refs, cutId}`
On the receiver, before import: compare this Mac's copy with the cut. Reply: `{refs: [{state}]}`, in request order.
- **States:**
  - `at_cut`: this Mac has nothing for these refs that the cut doesn't contain, or holds nothing at all. For an append-only stream, that means this Mac's copy is a byte prefix of the cut. For a row store, it means this Mac's newest rows for the ref are exactly those in the last cut this Mac exported or imported for it (for OpenCode, the newest message and part).
  - `advanced`: this Mac has something for these refs that the cut doesn't contain. That includes host data past an external `check`.
  - `unknown`.
- `advanced` anywhere stops the move and shows the operator the fork choice. `unknown` blocks too, and is never read as `at_cut`.
- May refuse `external_not_present`.
- A read.

### `agent_data.import {refs, cutId}`
On the receiver: read the cut through the door (`cut.read`, then `session.read` with the `cutId`, which needs no lease) into staging, and check every digest. Reply: `{items: [{sessionRef, endOffset, digest}]}`.
- **A released copy here:** if this Mac holds a released (tombstoned) copy of the same refs, which happens on a move back, the import replaces it. That's the expected case.
- **Refusals:** `incompatible_version`, `cut_not_ready`, `digest_mismatch`.
- **Step:** none → `imported`.

### `agent_data.activate {refs, cutId}`
On the receiver: make the staged data live. Reply: `{}`.
- Refuses `external_not_present` until the host data matches its `check`, and `busy` while something is running for these refs here (for example a run reopened on an archived session). Both are retried; the participant stays at `imported` meanwhile.
- Step: `imported` → `activated`.

### `agent_data.abort {refs, cutId}`
On either side, only before the move record reads `activated`. The source unfreezes and removes its durable freeze; the receiver drops its staging. Reply: `{}`.
- Aborting a `cutId` this participant never saw succeeds and does nothing.
- Refused `invalid_state` after `activated` (receiver) or `released` (source).
- Step: any earlier step → `aborted`.

### `agent_data.release {refs, cutId, holder}`
On the source, after the move record reads `activated`. The freeze becomes a durable tombstone naming the new holder (`holder` is its author pseudonym). Reply: `{enforced: bool}`.
- `enforced: true`: later local writes for these refs are refused, naming the holder, on the participant's own wire.
- `enforced: false`: the participant can't block writes (OpenCode without a per-prompt hook). It records the tombstone and hides the old copy, but anything written there later becomes a fork, which `diverged` finds on the next move back.
- Step: `exported` → `released`.

## A move back

The original Mac is just another receiver: `diverged`, then `import` and `activate` on the incoming cut, after entorhinal calls `cut.accept` so the participant can write to its door containers again. There is no separate resume op. A participant whose data is an append-only log (broca's WAL) accepts the incoming cut only if it extends the local copy byte for byte; otherwise `diverged` answers `advanced`.

## Settled elsewhere, not in this contract

- Project memories are shared by every agent in a project rather than owned by one. Whether and how they move is an AGENT_SYNC data-ownership question.
- A participant's own refusals to its local callers while frozen or released (for example broca's `session_moving`) belong on that participant's own wire.

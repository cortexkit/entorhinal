Chair rulings after round 0. These settle open question 1, "What admits an operator's agent-identity write?". Both entorhinal's owner and core's owner agreed to them on 2026-10-03.

R1 Until SUBC's `ck` gate lands, the operator row for agent identity is `reserved:prefrontal-core` only. Core keeps its existing agent-identity write ops (dispose, merge, rename, tag, labels, avatar, GitHub identity) and adds one for the hire cap. Each becomes a relay to entorhinal, sent under `reserved:prefrontal-core`, and core's primary-operator caller check stays exactly as it is today. Entorhinal admits every operator-row operation from `reserved:prefrontal-core`. The seven repointed operator scripts call core's relay ops, as they call core's ops today.

R2 An agent-identity write from `Direct` refuses with `direct_identity_write_not_admitted`, and the message names core's relay as the path. Admitting it would let any agent's shell write identity, which core's check prevents today. One exception: `grants.revoke` from `Direct` is admitted, because revoking only narrows authority.

R3 After the `ck` gate lands, a gated `Direct` write is admitted as the operator's, and core's relay ops are removed in a later cut. This row changes in an entorhinal release, never by detecting the gate at runtime. The panel already applied the same rule to the `reserved:callosum` row.

R4 The cut moves the store and the authority to entorhinal. Core's identity write ops become relays; they are not removed. Amend cutover step 3 and core's dependency item to say so. Amend the non-goal on proxying too: relaying operator writes through core's unchanged caller check is not a per-request read proxy, and it stays allowed.

R5 The relay contract, recorded in core's dependency item:
- core passes entorhinal's typed refusal through verbatim, never wrapped;
- the relay reply carries the new `(incarnation, generation)`;
- core's replica catches up from the change feed, so a read in the same turn may lag by one poll, and the generation in the reply lets a caller tell.

R6 Open question 1 is closed by R1 to R5. No open question remains.

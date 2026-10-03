Rulings from the campaign's owner (the chair, in the spec pipeline's terms) after review round 4. They narrow this campaign to the authority move and defer the agent-facing surface to a second campaign. After five review rounds the distinct blocking findings ran 33, 24, 26, 19, 21. Each fold resolved the previous round's findings and exposed new contradictions, nearly all in the consent, tool and grant surface: how the consent digest interacts with request keys, which error wins when several rules apply, partial grant revocation while a card is pending, and tool parameter conversion. That surface depends on `consent/v1`, which is still a design sketch, and until a consent capability is bound every write that needs one refuses. So this campaign delivers the authority move only. The agent-facing surface becomes a second campaign, specified against cingulate's real interface once it exists. The owner and core's maintainer agreed to this split on 2026-10-03.

R7 In scope:
- the agent identity store, with its invariants;
- both legacy id formats;
- the supervisor of each hire, copied at import and stored;
- the create request key on the row;
- the import, with its invariant checks and the cutover marker;
- the identity reads, including the fleet view's identity half with its unchanged token;
- the pulled change feed;
- `(incarnation, generation)` on every read that carries a generation;
- the attested principal, and Callosum's `origin` when present, recorded in the journal;
- operator writes admitted from `reserved:prefrontal-core`. Core keeps its existing agent-identity write ops as relays to entorhinal, with its primary-operator caller check unchanged. Each relay passes entorhinal's typed refusals through verbatim, and its reply carries the new `(incarnation, generation)`.

R8 Out of scope, moved to the second campaign:
- scoped agent tools (create_project, create_head, create_hire, dispose_agent) and the manifest's tool surface;
- reading `ScopeAttributes` on inbound stamps, including `flow_id` handling;
- consent cards, targeted-carrier routes to cingulate, the consent digest and `binding`, and the codes `consent_not_bound`, `consent_unavailable`, `consent_route_closed` and `consent_pending`;
- standing grants and `grants.list`, `grants.revoke`, `grants.would_ask` and `grants.offer`;
- the hire cap, and a supervisor retiring its hire without a card;
- admitting `reserved:callosum`.
Delete every constraint, acceptance item and check-order step that exists only for these features. Keep a constraint only where it stops this campaign's code from ruling them out later. For example, the store must still be able to hold a grant, and admission must still be able to gain a scoped-agent row.

R9 Admission in this campaign:
- agent-identity mutations are admitted only from `reserved:prefrontal-core`;
- a `Direct` agent-identity mutation refuses `direct_identity_write_not_admitted`, and the message names core's relay as the path. Any local process, an agent's shell included, reaches entorhinal as `Direct`, so admitting it would weaken core's existing check. The earlier exception admitting `grants.revoke` from `Direct` no longer applies, since grants are out of scope;
- every other principal refuses `write_not_permitted`;
- before the cutover marker exists, every agent-identity mutation refuses `authority_not_cut_over`;
- project and workspace operations keep today's gate unchanged.
No agent-initiated write path exists in this campaign, so nothing here refuses for lack of consent.

R10 Reads are open to every principal, which is how entorhinal admits reads today. Before the cutover marker exists, a resolve of a well-formed but absent agent id returns `unknown`, never `authority_not_cut_over`.

R11 Moving entorhinal to subc-protocol 0.29 is not part of this campaign: it already shipped on its own. Entorhinal 0.1.14 (commit 192f220) runs subc-protocol 0.29.0 with subc-client-rs 0.26.1, so this campaign starts from those versions and must not move off them.

R12 The deferred list, kept here so nothing drops silently between the two campaigns. The second campaign starts from exactly these items, specified against cingulate's real `consent/v1` interface once it exists:
- scoped agent tools for the janitor and heads: create workspace, create project, create head, create hire, dispose agent; their request keys, and how each tool's parameters convert to the matching journal op;
- reading the calling agent from the inbound `ScopeStamp`, including refusing flow-scoped calls;
- consent: card kind `entorhinal.identity_request`, targeted-carrier routes to cingulate opened only inside a live inbound tool call, the request digest and opaque `binding`, the pending and expiry rules, and the codes `consent_not_bound`, `consent_unavailable`, `consent_route_closed` and `consent_pending`;
- standing grants: per-workspace scope by default, fleet-wide only as an explicit option, minted only from an answer to a card entorhinal raised, served behind `grants.list`, `grants.revoke`, `grants.would_ask` and `grants.offer`;
- the per-project cap on live hires, default 10 and operator-changeable, and a head retiring its own hire with a notice and no card;
- admitting `reserved:callosum` for app-initiated creates. When those ops are exposed to the apps they go into Callosum's `phone` profile only, never `sim`;
- once SUBC's `ck` gate exists, admitting a gated `Direct` write as the operator's, and removing core's relay ops in a later cut (R3).

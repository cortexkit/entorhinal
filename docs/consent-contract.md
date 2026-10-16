# Consent contract

Agents can ask entorhinal to create workspaces, register projects, create
project heads and hire agents. These are agent tools, a separate surface from
the operator's and the executive's management operations. A tool write lands
only with the operator's consent: an answer to a consent card shown on the
operator's phone, or a standing grant from an earlier "approve always" answer.
This contract covers the tools, requests, cards, grants, the re-check before a
write lands, the audit and the hire cap.

Code: `crates/entorhinal-module/src/tool_provider.rs` (tools and caller
admission), `tool_writes.rs` (the write path), `consent.rs` and
`consent_client.rs` (cards, sent through Cingulate), `permissions.rs`
(inventory, audit, hire caps); `crates/entorhinal-core/src/agent/consent.rs`
(the local consent tables, grants, audit and cap). Tests are in
`tool_provider_tests.rs`, `tool_writes_tests.rs`, `consent_tests.rs`,
`permissions_tests.rs` and `crates/entorhinal-core/src/agent/consent_tests.rs`.

## 1. Tools

entorhinal provides `tool-provider/v1`. The tool set is closed:

| Tool | Caller role | Writes | Consent |
|---|---|---|---|
| `projects_create_workspace` | `assistant` | yes | A card every time; no standing grant exists for it. |
| `projects_register` | `assistant` | yes | A card, or a `workspace_projects` grant for the workspace. |
| `agents_create_head` | `assistant` | yes | A card, or a `workspace_heads` grant for the project's workspace. |
| `agents_hire` | `head` | yes | A card, or a `project_hires` grant for the head's project. |
| `agents_retire_hire` | `head` | yes | None: a head may retire its own live hire (`own_hire`). |
| `agents_list_hires` | `head` | no | None. |

## 2. Caller admission

The caller is the agent named in the daemon's scope stamp for the route; a
tool argument never names the caller. On every call, including retries:

1. The route must carry a scope stamp, authorized by its owner, that is not a
   worker scope, not a flow scope, and not a run that does not delegate.
   Otherwise `caller_forbidden`.
2. The stamped agent must exist (`unknown_agent`), be live (`gone`), and have
   the tool's role (`role_forbidden`).
3. If this Mac's cached claim for the agent failed verification,
   `claim_integrity_failed`; if it names another Mac at epoch 1 or higher,
   `held_elsewhere`. A standing grant does not override either.
4. A write tool requires `request_key`: 1–128 bytes, each printable ASCII
   (`0x20`–`0x7e`); otherwise `invalid_request_key`. This is checked before
   any other argument.

The [catalog tools](catalog.md#agent-tools-and-consumer-protocol) reuse these stamp,
claim and consent checks but admit every live agent role for self-installation.
`catalog_install {plan_id}` is the request-key exception: the plan id itself is
the persisted consent/replay key, so it accepts no additional `request_key`.

Admission is decided from the stamp saved when the route was bound. A route
that closes loses its authority.

Tests: `every_tool_admits_only_its_table_role_before_key_or_arguments`,
`bind_captures_scope_epoch_and_mode_and_route_gone_drops_authority`,
`agent_tools_refuse_cached_claims_held_elsewhere_even_with_standing_grants`,
`every_write_validates_printable_ascii_request_keys_before_other_arguments`.

## 3. Requests

A request is identified by `(agent, tool, request_key)` and bound to a digest
of its arguments.

1. Reusing a request key with different arguments refuses
   `request_key_conflict`, even after the first request finished.
2. Reusing it with the same arguments returns the stored terminal outcome
   (applied or refused) without asking again, or resumes a request still
   awaiting consent.
3. One request key runs at most one card and commits at most one outcome, even
   under concurrent calls; a concurrent call gets `consent_pending`.
4. Validation refusals (unknown workspace, existing project, cap reached and
   so on) happen before any card and leave no request row.

Tests: `request_keys_are_ascii_namespaced_and_conflicts_survive_terminal_state`,
`same_key_concurrency_uses_one_card_and_a_single_terminal_commit`,
`validation_refusals_raise_no_cards_or_request_rows`,
`request_namespaces_and_grant_classes_do_not_cross_authorize`.

## 4. Cards

A card is sent to Cingulate (the module that shows consent cards on the phone)
on a route opened under the requesting agent's own scope stamp. The card names
the requesting agent, the action and the target, defaults to `deny`, denies on
expiry, and expires after 10 minutes. Its options are closed:

| Option | Offered for | Effect |
|---|---|---|
| `approve_once` | every card | Authorizes this request only. |
| `approve_always_workspace` | `projects_register`, `agents_create_head` | Authorizes this request and records a workspace grant. |
| `approve_always_project` | `agents_hire` | Authorizes this request and records a project grant. |
| `deny` | every card | Refuses. |

A tool call waits up to 115 seconds. Outcomes:

| Outcome | When |
|---|---|
| applied | Approved, and the re-check in section 6 passed. |
| `consent_pending` | No answer yet. Retry with the same request key; the retry reuses the same card. |
| `consent_declined` | The operator chose `deny`. |
| `consent_expired` | The card expired. |
| `consent_lost` | The answer can no longer be trusted or found: it was settled more than 24 hours ago, the request's scope changed, or a card whose creation is uncertain was never confirmed within 11 minutes. |
| `consent_unavailable` | Cingulate could not be reached for a new request. The request row is removed, so a retry starts fresh. |
| `invalid_reply` | The answer is malformed, or an "always" choice does not match the action's grant class and scope. |

`consent_declined`, `consent_expired`, `consent_lost`, `invalid_reply` and the
re-check refusals are terminal: they are recorded, audited and replayed to any
retry. An approval is never inferred from a timeout or an expiry.

Tests: `every_card_matches_frozen_cingulate_decoder_and_external_key_vector`,
`await_settlement_and_expiry_are_not_pending_or_implicit_approval`,
`deny_expiry_and_pending_retry_are_terminal_and_apply_once`,
`preterminal_capture_timeout_returns_pending_then_uses_saved_answer_without_resend`,
`recorded_retention_loss_and_missing_card_refuse_without_reraising`,
`unoffered_always_approvals_are_terminal_refusals_and_replay_without_grants`,
`always_approvals_validate_grant_class_and_saved_scope_kind`.

## 5. Grants

A grant is held by one agent and has a class and a scope. Classes are closed:

| Class (stored) | Inventory scope | Covers |
|---|---|---|
| `register_project` | `workspace_projects` | `projects_register` in one workspace |
| `create_head` | `workspace_heads` | `agents_create_head` in one workspace |
| `hire` | `project_hires` | `agents_hire` in one project |

1. A grant is created only by an approved "always" answer whose class and scope
   match the saved action. Approving again for the same holder, class and
   scope refreshes the existing grant in place and keeps its id.
2. A grant does not expire. It stops covering requests when it is revoked,
   when its holder is retired or merged, or when its workspace or project is
   removed. Rows stay for the inventory history and the audit.
3. Revocation takes effect for the next request; it does not undo a write
   already made.
4. Grants are machine-local. They are never written to the identity log and
   never authorize anything on another Mac.

`permissions.list {agent_id?}` lists live grants on this Mac, labelled with
workspace or project names. `permissions.revoke {id}` revokes a grant without
prompting and returns `revoked`, or `not_found` for an unknown or already
ended grant. Both admit only `Direct` and `reserved:cingulate`; everyone else,
agents included, gets `permissions_operator_required`. entorhinal is a
provider of `permissions/v1`; aggregating providers for the phone is
Cingulate's job.

Publisher grants are a separate local inventory scope, `catalog_publisher`, not
one of the agent-tool grant classes or card options above. Their holder is the
placement agent and their subject label is the publisher fingerprint. Coverage
requires a live matching fingerprint/operation/placement row, required
capabilities within its ceiling, and no scripts; an empty ceiling is valid for
token-free packages. Root-certified CortexKit packages do not receive grants.
The operator's `ck packages publisher trust` authorizes install only, using
authenticated pinned manifests and operator confirmation. Pins are supplied by
a local trait; until a production pin source exists it refuses
`manifest_unavailable`, rather than trusting caller-supplied capabilities.
Permissions admission is unchanged. Revoke sets `revoked_at_ms`, retains the row,
and appends `grant_revoke`; refresh keeps its id and unions its capability ceiling.
Grant writes append `grant_insert` or `grant_refresh`. Each catalog audit row
includes fingerprint, operation, placement, ceiling and acting principal, and is
not served by `agent.audit_list`. Publisher grant and audit state stays local
through export and rebuild. No new consent-card option is wired here.

Tests: `grants_refresh_in_place_and_revocation_blocks_a_new_write`,
`provider_drops_grants_when_holder_or_subject_ends`,
`provider_revoke_vectors_are_idempotent_local_and_prompt_free`,
`provider_admission_precedes_decode_and_never_prompts`,
`request_conflict_and_crash_after_commit_replay_do_not_remint_revoked_grant`,
`consent_state_is_local_persistent_and_not_rebuilt_or_exported`.

## 6. Re-check before the write

An answer is evidence that the operator approved a specific action at a
specific moment. Before the write commits, inside the same transaction:

1. The requesting agent must still exist and be live, with the same role.
2. The action is prepared again and compared with the saved, approved plan. A
   changed target or placement refuses `consent_stale`.
3. A grant used for authorization must still be live and have the plan's
   class and scope.
4. The hire cap is checked again (`hire_cap_reached`), even with a grant.
5. A retirement target must still be the caller's live hire (`not_your_hire`).
6. Conflicts found at commit time (`name_conflict`, `head_exists`,
   `project_exists`, `workspace_exists`, `invalid_role_shape`) are recorded as
   `consent_stale`.

The write, its audit row, any new grant and the request's terminal outcome
commit together. If the audit row cannot be written, nothing commits and a
retry applies the write once.

A tool write that changes shared state goes through the shared-log writer like
any other ([identity-log-contract.md](identity-log-contract.md)). Its entry
carries the authorization (card id and answer, grant id, or `own_hire`), so
`verify` and `rebuild` on any Mac need no live card or grant.

Tests: `commit_rechecks_caller_role_liveness_cap_grant_and_own_hire`,
`assistant_role_and_grant_scope_are_rechecked_inside_commit`,
`hire_cap_is_rechecked_in_transaction_even_with_a_standing_grant`,
`audit_failure_rolls_back_mutation_journal_grant_and_terminal_then_retry_applies`,
`journal_authorization_variants_survive_export_and_replay_without_local_authority`,
`own_hire_retirement_journal_carries_the_authorization`.

## 7. Audit

Each tool write that commits a terminal outcome (applied, or refused after
consent was requested) appends one audit row: `row_id`, `time_ms`,
`agent_id`, `action`, `target`, `authorization` and `outcome`. Refusals in
sections 2 and 3 come before any consent and leave no row.

`agent.audit_list {since_ms?, limit?, cursor?}` pages the audit newest first,
ordered by time then row id, both descending. `since_ms` is inclusive.
`limit` defaults to 200 and must be 1–200. `next_cursor` is opaque and present
only when more rows exist; pass it back unchanged to continue, including across
rows with the same timestamp. A malformed cursor refuses `invalid_request`.
Only `Direct` and `reserved:callosum` may read it; others get
`audit_operator_required`. The audit is machine-local.

Tests: `audit_pages_visit_tied_timestamps_once_and_since_is_inclusive`,
`audit_wire_pages_tied_timestamps_once_with_inclusive_since`,
`audit_admission_is_operator_or_callosum_only_before_decode`.

## 8. The hire cap

1. Each project has a hire cap on each Mac. With no cap set, it is 10.
2. The count is every live `hiree` in the project, however it was created.
3. Only `agents_hire` is gated: at or above the cap it refuses
   `hire_cap_reached`, with or without a grant. Operator creation, the
   executive's relay, imports and identities received from other Macs are not
   gated.
4. A cap of 0 stops agent hiring. Lowering the cap below the current count
   retires nobody.
5. `agent.set_hire_cap {project_id, cap}` is `Direct` only
   (`direct_caller_required` otherwise) and always asks for Touch ID. A
   negative cap refuses `invalid_request`; an unknown project refuses
   `not_found`. Caps are machine-local.

Tests: `hire_cap_defaults_to_ten_and_operator_creation_is_not_gated`,
`default_cap_refuses_the_eleventh_live_hire_even_with_a_grant`,
`hire_cap_admission_validation_and_confirmation_guard_every_write`,
`hire_cap_cancelled_or_expired_confirmation_never_writes`,
`hire_card_grant_cap_and_own_retirement_acceptance`.

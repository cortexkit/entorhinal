---
rounds: 0
---
These rulings answer round 0's needs_evidence questions from core's source at prefrontal `873870be8`, read directly, and mint the slice plan. Round 0 folded all its blockers. They were all exact wire details, of the kind each slice's tests pin best. Everything else in the round's fold stands.

R40 Project and workspace ids. Core's `validate_project_id` and `validate_workspace_id` (`prefrontal-core-store/src/agent_registry.rs:1379-1395`) trim with Unicode 15.1 and refuse an empty result, or one over 512 bytes (`MAX_SCOPE_BYTES`, line 33), with `invalid_project_id` or `invalid_workspace_id`. They check no character set and no prefix. No other variant maps onto those codes. Entorhinal applies the same rule to the `project_id` and `workspace_id` of agent ops. Project ops keep their own rules. On create, the order is the same as core's (`agent_registry_ops.rs:3026-3049`, then `prepare_agent_create` at `agent_registry.rs:1784-1822`):
1. id validity;
2. role shape (`invalid_role_shape`);
3. name (`invalid_name`);
4. tag (`invalid_tag`);
5. binding resolution;
6. the name claim.

R41 Workspace match in `agent.resolve_name`. `row_workspace_matches` (`agent_registry_ops.rs:1174-1184`) compares only the row's stored `workspace_id`, passed through `normalize_agent_name` (`normalized_workspace_id`, `1168-1172`). A row with a NULL `workspace_id` matches only the scope `"global"`. It doesn't consult project placement. Entorhinal does the same.

R42 Avatars and GitHub identity.
- **Genome:** core checks only the genome's length, which must be exactly 2048 characters for `creature.classic`. It does not check that the characters are hex (`agent_registry_ops.rs:3276-3285`). A missing `type` or a wrong length refuses `invalid_request`. Entorhinal matches this and adds no hex check, since a stricter rule would refuse what core accepts today.
- **Avatar in replies:** `AgentAvatar` (`agent_registry.rs:638-647`) serialises as `{genome, type, version}`, camelCase, with `type` and `version` omitted when absent, not null. The `avatar` in `agent.set_avatar`'s reply keeps that shape. The agent row's `avatar` object keeps the spec's own null rule.
- **GitHub identity:** a `github_identity` that doesn't decode as the tagged enum (an unknown `kind` or an unknown field) refuses `invalid_request` (`decode_github_identity`, `agent_registry_ops.rs:844-851`). One that decodes but fails `validate_github_identity` (`agent_registry.rs:1507-1552`) refuses `invalid_github_identity`. That covers `app_id <= 0`, an `installation_id` present and `<= 0`, an empty `app_slug`, `credential_ref`, `coauthor_line` or `login`, and a `client_id` or `coauthor_line` that is present but empty.

R43 Every slice that ports a core validator or serialiser cites the core function it copies, and pins core's behaviour with a test whose expected values come from that function, not from entorhinal's own output. That is the general form of R35, and it applies to any core fact the spec cites but doesn't spell out.

R44 The intent, non_goals and open_questions sections need no revision for R40 to R43: keep their current text exactly.

---
title: "Operator agent commands: `ck agents` and Direct agent writes confirmed by operator presence"
status: draft
rounds_cap: 3
mint: auto
integration_ref: "main"
evidence:
  include:
    - "crates/entorhinal-module/src/main.rs"
    - "crates/entorhinal-module/src/agent_ops.rs"
    - "crates/entorhinal-module/src/cli.rs"
    - "crates/entorhinal-module/src/shared_write.rs"
    - "crates/entorhinal-core/src/agent/store.rs"
    - "crates/entorhinal-core/src/agent/reads.rs"
    - "crates/entorhinal-core/src/agent/names.rs"
    - "crates/entorhinal-core/src/agent/validate.rs"
---

## intent
The operator decided that agent identity must not depend on prefrontal-core: people may run a different orchestrator. Today every agent write in entorhinal is admitted only from `Reserved("prefrontal-core")`, and `Direct` callers are refused with `direct_identity_write_not_admitted` (`crates/entorhinal-module/src/main.rs`, `authorize_write`). So nobody can create, rename, retire or retag an agent without core.

Direct can't simply be admitted. The operator's terminal, the desktop app and every agent shell on the Mac all reach entorhinal as `Direct`, and the sandbox that would separate them is optional on self-hosted machines. Admitting bare Direct would let any agent mint agents.

The subc daemon is adding an operator-presence check: during a request, a module calls the daemon op `operator.confirm { summary, route }`. The daemon shows a Touch ID or password prompt with the module's identity and the summary text, and answers confirmed or refused. Nothing is minted or transferable. The approval is bound to the request because the module calls it inside that request, and the person approves that exact text.

End state:
- A Direct caller (the `ck agents` CLI, the desktop app, or anything else local) can create, rename, retire, retag and relabel agents. Each write lands only after the operator approves its exact description at the prompt.
- `ck agents` is a new operator CLI face for those writes and for reading agents.
- Core's relay (`Reserved("prefrontal-core")`) keeps working unchanged, with no prompt.
- Nothing about this works before the identity cut: before the `agent.cutover` marker, every agent write still refuses `authority_not_cut_over`, before any prompt.

## non_goals
- Avatar, GitHub identity and merge writes from Direct callers. They stay refused for Direct, as today.
- Agent-initiated writes (heads creating hires, consent cards through cingulate). That's the deferred phase 2.
- Residence: which session an agent lives in is core's, bound after core sees the agent through its replica.
- Writes before the identity cut, or any bridge into core's store.
- Confirming reads. Reads stay open to every principal, with no prompt.
- Admitting callers by callosum attribution. `CallOrigin` is attribution only, never authority.

## constraints
### Which writes, from whom
- Operator-confirmable methods: `agent.create`, `agent.rename`, `agent.dispose`, `agent.update_tag`, `agent.set_labels`.
- Admission order for those methods, all before any prompt:
  1. A flow-scoped route refuses `flow_scope_not_admitted`, as today.
  2. `Reserved("prefrontal-core")` is admitted with no prompt, as today.
  3. `Direct` is admitted conditionally: it needs operator confirmation (below).
  4. Every other principal refuses `write_not_permitted`, as today.
- `agent.set_avatar`, `agent.set_github_identity` and `agent.merge` from `Direct` keep refusing `direct_identity_write_not_admitted`.

### Before the prompt
A prompt is shown only for a write that would land if approved now. Every refusal that doesn't depend on the operator comes first, and none of them prompts:
- **Missing request key:** `request_key_required`, as today.
- **Replayed key:** a request key that already landed with the same request returns its cached reply with no prompt. A key reused with a different request refuses as today.
- **Before the cut:** with no `agent.cutover` marker, the write refuses `authority_not_cut_over`.
- **Validation:** every validation the write performs (names, role shape, project and workspace placement, `uq_live_head`, name claims, supervisor rules, unknown agent) runs against current state on the read connection, without taking the writer lock or writing anything. The worker chooses how to do this exactly (for example, running the mutation in a transaction that is always rolled back), but it must not duplicate the validation rules.
- **Busy:** at most one operator confirmation may be outstanding in the process. A second confirmable Direct write during a prompt refuses `operator_confirmation_busy` at once, so an agent can't stack prompts on the operator.
- **Summary too long:** if the full summary doesn't fit the daemon's limit, the write refuses `operator_summary_too_long`. The summary is never truncated, because truncation would approve text the person didn't see.

### The summary
- One line, built only from the validated request after normalization, naming the write in full. Agent names, tags and labels appear quoted with control characters escaped. Agents are named by display name and id.
- Examples: `create head agent "CLOUD" for project pj-afb8476f67f570f8 with tag "cloud lead"`, `rename agent "Ada" (agent_0123…) to "Grace"`, `retire agent "Ada" (agent_0123…)`.
- The cap is 200 characters, set by the daemon and firm: the macOS prompt clips longer text itself, so the person would approve text they didn't see. It's one constant in one place. The `operator_summary_too_long` refusal says to split the write, for example setting labels in two calls.

### The prompt
- Entorhinal calls the daemon through its own trait, `confirm_operator(summary, caller_route) -> Result<(), OperatorConfirmError>`. The errors are `Declined`, `PresenceUnavailable` and `Unsupported`. Tests use a stub.
- No store lock or writer lock is held while the prompt is up. Other reads and writes, including other agent writes from core, proceed normally.
- The result maps to a refusal: `Declined` → `operator_declined`. `PresenceUnavailable` → `operator_presence_unavailable`. `Unsupported` (an older daemon) → `operator_presence_unavailable`, never allowed.
- Until subc-client-rs publishes the channel-0 helper, the production implementation returns `Unsupported`, so Direct writes keep refusing exactly as today. Wiring the real helper is its own slice.
- The request deadline covers the prompt: at least 110 s for a confirmable Direct write, measured from receipt. A prompt still open at the deadline counts as declined.

### After approval
- The write then takes the writer lock and runs exactly as it would for core: full validation again, then commit (or, with the identity log enabled, the shared write path). If state changed while the prompt was up and the write now fails, that refusal is the answer. The approval is not retried and not saved.
- The journal row records principal `direct`, as today, and adds `"operator_confirmed": true` to the payload. Rows written for core's relay are byte-identical to today.
- A confirmed `agent.create` from Direct sets no supervisor unless the request names one. Supervisor rules are unchanged.

### The `ck agents` face
- `ck-agents` and `ckdev-agents` select a new `Face::Agents`, through the same prefix rule as `projects` and `workspaces`.
- Verbs:
  - `create <name> --role <assistant|workspace-head|head|hiree> [--project <id>] [--workspace <id>] [--tag <text>] [--supervisor <agent>]`
  - `rename <agent> <new-name>`
  - `retire <agent>`
  - `tag <agent> <text>`
  - `labels <agent> <label>...` and `labels <agent> --none`
  - `list [--project <id>] [--workspace <id>]`
  - `show <agent>`
- `<agent>` is an agent id or a name. A name resolves through the existing read path first, and an ambiguous or unknown name refuses before any write is sent.
- Each write generates a fresh random request key. `--request-key <key>` reuses one, for a scripted retry.
- Write calls wait at least 120 s, to cover the prompt. Before waiting, the CLI prints one line to stderr saying that approval is pending on this Mac.
- Output is human-readable by default and the raw reply with `--json`, like the other faces. Refusal codes are shown verbatim, with a one-line explanation for `operator_declined`, `operator_presence_unavailable`, `operator_confirmation_busy` and `authority_not_cut_over`.

### Unchanged
- Core's relay path: no prompt, no new payload field, same replies.
- Reads, project and workspace writes, and the identity log are untouched. With the log disabled, the golden byte-invariance test passes unchanged.

## acceptance_sketch
Each test must fail when the behaviour it names breaks. Each slice reports one deliberate break per test, and shows the failure.
1. **Confirmed Direct writes land.** After the cut, Direct `create`, `rename`, `dispose`, `update_tag` and `set_labels` each call the stub once, with the exact expected summary, and land once confirmed. The journal row has principal `direct` and `"operator_confirmed": true`.
2. **Refused approvals write nothing.** `Declined`, `PresenceUnavailable` and `Unsupported` each refuse with their code, and leave the journal, the generation and the agent tables unchanged.
3. **No prompt for a doomed write.** None of these calls the stub, and each refuses with its own code: no request key; before the cut; an invalid name; a live-head conflict; an unknown agent; a project not in a workspace; flow scope; a summary over the cap. A replayed request key returns its cached reply without calling the stub.
4. **Core unchanged.** The same writes from `Reserved("prefrontal-core")` never call the stub, and their journal rows are byte-identical to today.
5. **Still refused for Direct.** Direct `set_avatar`, `set_github_identity` and `merge` refuse `direct_identity_write_not_admitted` without calling the stub.
6. **No lock across the prompt.** While a Direct create waits in a stub that holds the prompt open, a project write and a core agent write both complete. Assert the order, not the time.
7. **One prompt at a time.** While one prompt is open, a second Direct confirmable write refuses `operator_confirmation_busy` without calling the stub. After the first finishes, a new one prompts normally.
8. **State changes during the prompt.** While a Direct create of name N waits for approval, core creates N. After approval, the Direct create refuses with the name-claim conflict, and nothing else is written.
9. **Deadline.** A stub that never answers ends in `operator_declined` at the request deadline. Use a paused clock and assert the outcome, not elapsed time.
10. **CLI.** Each `ck agents` verb builds the expected method and params. A name resolves to an id first, and an ambiguous name refuses before any write. Writes carry a request key, and `--request-key` is used verbatim. The face resolves from `ck-agents` and `ckdev-agents`. One end-to-end test drives `ck agents create` through the handler with a confirming stub.
11. **Disabled is today.** The golden byte-invariance test passes unchanged.

## open_questions
- None on the design. Deployment notes for reviewers:
  - The subc-client-rs channel-0 helper isn't published yet. SUBC publishes the types before the daemon side, and S3 wires it then. Until then, the production implementation of the trait returns `Unsupported`.
  - The `ck-agents` links (`~/.local/share/cortexkit/bin/ck-agents` and `~/.local/bin/ck-agents`) are created by SUBC when the card is placed, and verified with `place-module.sh --path-face ck-agents`. Nothing in this repo creates them.

## slice_hints
- S1: module admission and the confirmation flow (pre-checks, busy guard, summary, no-lock prompt, re-validate and apply, payload flag) behind the trait, with a stub and the `Unsupported` production implementation. Acceptance 1 to 9 and 11.
- S2: the `ck agents` face, depending on S1. Acceptance 10.
- S3, after subc-client-rs publishes the helper: a production trait implementation over the channel-0 op, with its protocol pin.

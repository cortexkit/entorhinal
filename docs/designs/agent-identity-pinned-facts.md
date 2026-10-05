# Agent identity move: facts read from core and the pinned SDK

The agent-identity spec leaves some behaviour to "core's function, quoted" or "the
SDK's envelope". This file records those facts, read from source on 2026-10-05,
so the spec and its slices start from what the code does instead of each
reader guessing.

Sources:

- prefrontal at `873870be8`. Its files are cited by path and line, and the
  campaign's evidence includes those ranges, so a reviewer reads the source
  itself.
- `subc-client-rs` 0.26.1 and `subc-protocol` 0.29.0, the versions entorhinal
  pins. These live in the cargo registry, outside any repository, so the parts
  that matter are quoted here verbatim.

## The pinned SDK

### Refusals can carry structured data, under the name `detail`

`subc-client-rs` 0.26.1, `src/lib.rs`:

```rust
pub enum HandlerOutcome {
    /// Send a Response frame carrying these bytes.
    Response(Vec<u8>),
    /// Send an Error frame carrying an [`ErrorBody`] with this code and message.
    Error { code: String, message: String },
    /// Send an Error frame with stable machine-readable detail.
    ErrorWithDetail {
        code: String,
        message: String,
        detail: serde_json::Value,
    },
    /// The handler emitted stream data with [`RequestCtx::emit`]; the serve code
    /// sends the StreamEnd terminal frame.
    Streamed,
}
```

The serve loop sends it as `ErrorBody::new(code, message).with_detail(detail)`.
`subc-protocol` 0.29.0, `src/lib.rs`:

```rust
pub struct ErrorBody {
    pub code: String,
    pub message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detail: Option<serde_json::Value>,
}
```

So a structured refusal object reaches the wire with no pin change. The field
is called `detail`. Today entorhinal only ever returns `HandlerOutcome::Error`.

### Nothing carries a Callosum origin

The structures a module handler can see are `RequestCtx` (`handle`, `corr`,
`ver`, `egress`, `module_handle`, `cancelled`) and `RouteBindRequest`:

```rust
pub struct RouteBindRequest {
    pub handle: RouteHandle,
    pub target: RouteTarget,
    pub identity: BindIdentity,
    pub principal: Option<Principal>,
    pub consumer_capabilities: Option<Vec<String>>,
    pub role_versions: Option<std::collections::BTreeMap<String, String>>,
    pub admission_facts: Option<serde_json::Value>,
    pub scope: Option<ScopeStamp>,
}
```

`ScopeStamp` holds `owner`, `ref`, `scope_epoch`, `kind`, `parent`,
`parent_state`, `attributes` and `owner_authorized`. `ScopeAttributes` holds
`agent_id`, `delegates` and `flow_id`. `Principal` is
`Reserved { module_id } | Direct | Unverified`. None of them carries an
origin, a device or anything Callosum attests. The envelope has a
`DAEMON_ORIGIN` flag bit, but `RequestCtx` doesn't expose ingress flags, and
that bit isn't Callosum's origin anyway.

### Handlers run concurrently, with no per-request deadline

The `ModuleHandler::handle` doc says: "Each request runs in its own task so one
slow handler cannot head-of-line-block another route." The serve loop spawns a
task per data request and awaits `handle` with no timeout. A semaphore of
`HANDLER_TASK_CAPACITY = 64` bounds those tasks. The only fixed deadlines in
the SDK are the 2 s authentication deadline and the 10 s catalog-update
timeout, and neither applies to handler requests. On the calling side,
`DEFAULT_CALL_TIMEOUT` is 30 s. So a 25 s long poll fits under a default call,
and while it waits it holds one of the 64 handler slots.

## Core's decoders of entorhinal's project reads

Core decodes only two of entorhinal's project reads. `resolve` is decoded by
hand from a `serde_json::Value` and then as `ProjectScope`. `enumerate` is
decoded as `ProjectsEnumerateResult`. Neither type, nor any nested type, uses
`deny_unknown_fields`, so an extra top-level `incarnation` is ignored
(`crates/prefrontal-core-module/src/projects_consumer.rs:528-538`, `708-756`,
`808-860`, `899-905`). Core has no decoder for `resolve_project_id`,
`journal_tail`, `trust` or `verify`.

## Store error names are not wire codes

"Use core's code" means core's module reply, which its relay passes to clients,
not `AgentRegistryError::code` in the store. At prefrontal `873870be8`,
`crates/prefrontal-core-module/src/agent_registry_ops.rs`, `registry_error`
maps invalid project and workspace ids as follows:

```rust
        AgentRegistryError::InvalidProjectId => {
            RegistryOpError::new("invalid_role_shape", "invalid project_id")
        }
        AgentRegistryError::InvalidWorkspaceId => {
            RegistryOpError::new("invalid_role_shape", "invalid workspace_id")
        }
```

Id validity is checked before role shape, as core does, so a request with both
an invalid id and a role that doesn't fit gets the invalid-id message. Both
refusals use `invalid_role_shape`, and the message tells them apart. List and peer-roster id filters instead use `invalid_request`.

## Name normalisation

`normalize_agent_name` is at
`crates/prefrontal-core-store/src/agent_registry.rs:1285-1322`, with its helpers
at `1237-1283`. It works in this order:

1. Refuse a disallowed character, reporting its codepoint.
2. Apply ICU NFC.
3. Trim Unicode 15.1 whitespace to get the stored name.
4. Apply ICU full, non-Turkic case folding to the NFC form, then trim, to get
   the normalised name. The folded output is not normalised again.
5. Refuse `Empty` when either trimmed name has zero scalars, and `TooLong` when
   either has more than 24 (`MAX_NAME_SCALARS`).

`NAME_NORMALIZATION_VERSION` is `1`. The store crate pins `icu_casemap =
"=1.5.0"` and `icu_normalizer = "=1.5.0"`
(`crates/prefrontal-core-store/Cargo.toml:21-22`).

Core pins input/output pairs in tests at `agent_registry.rs:7533-7661` and
`7733-7763`:

- NFC and case folding: `" Cafe\u{301} "` is stored as `"Café"`, and `"Straße"`
  normalises to `"strasse"`.
- Special folds: `"İ"` normalises to `"i\u{307}"`, and `"ẖ"` to `"h\u{331}"`.
- Collisions: `"\u{3000}Alice\u{00A0}"` collides with `"alice"`.
- Length: 26 raw scalars that compose to 13 are accepted, and 13 `ß`, which
  fold to 26, are `TooLong`.
- Refusals: whitespace-only input is `Empty`, and 25 `x` are `TooLong`. The
  codepoints `0x200B`, `0x202E` and `0x2060` are refused, while `"Алиса"` is
  kept.

These tests can be copied verbatim.

## Core's agent tables

The relevant migrations are `079_hire_identity_mapping.sql`,
`080_agent_github_identity.sql`, `082_wake_delivery.sql`,
`109_agent_generation.sql`, `112_agent_avatar.sql` and `126_agent_labels.sql`.
Another file, `126_harness_supervision.sql`, also starts with 126, but it
alters `harness_instance`, not `agent`. Together with `076`, the set
references nothing created outside it.

After 082's rebuild and the later additions, `agent` has these columns:

- identity: `agent_id`, `name`, `name_normalization_version`, `tag`, `role`,
  `project_id`, `workspace_id`, `persona_ref`, `name_version`,
  `github_identity_json`, `generation`, `avatar_genome`, `avatar_type`,
  `avatar_version`, `labels_json`;
- wake: `wake_policy_json`, `wake_policy_version`;
- residence: `residence_machine_id`, `residence_harness`,
  `residence_address_json`, `residence_epoch`, `residence_state`;
- lifecycle: `sleep`, `created_at_ms`, `updated_at_ms`, `terminal_reason`,
  `terminal_at_ms`, `merged_into_agent_id`.

Two changes from 076 matter:

- **082 dropped the rule that a head or hiree has no workspace.** Its CHECK
  only requires `project_id IS NOT NULL` for those roles
  (`082_wake_delivery.sql:33-37`, against `076_agent_registry.sql:24-28`), and
  core writes `workspace_id` on heads.
- **The `residence_machine_id` foreign key to `machine_binding` survives** at
  `082:21`.

`hire_identity_mapping` (079) has only `hire_id`, `agent_id`, `hire_class`
(`live_runtime` or `concluded`) and `created_at`. **Core records no supervisor
anywhere.** Its insert path (`agent_registry.rs:3653-3668`) doesn't check the
role.

## Roles, workspaces and name namespaces

Core's wire roles are `assistant`, `workspace_head`, `head` and `hiree`
(`agent_registry_ops.rs:449-456`, snake_case). Nothing named "primary" or
"janitor" exists as a role.

`agent.create` resolves the project's workspace for a head or hiree before
anything is claimed. It refuses `unresolved_workspace` ("project has no
resolved workspace") when the project has no placement, and `activation_gap`
when the workspace authority is unavailable (`agent_registry_ops.rs:1129-1144`,
`3038-3055`). `namespace_for_create` (`agent_claims.rs:44-67`) then picks the
name's namespace:

- `assistant`: kind `assistant`, key `"global"`;
- `workspace_head`: kind `workspace`, keyed by the row's own `workspace_id`;
- `head` and `hiree`: kind `workspace`, keyed by the resolved workspace id,
  which is also stamped into the row's `workspace_id` (`agent_claims.rs:383-400`).

So core never creates a head or hiree on an unplaced project. A head can still
end up on an unplaced project later, if the project loses its placement after
the head was created.

## Core's agent op bodies

All request structs are `deny_unknown_fields`
(`agent_registry_ops.rs:488-732`). Field names are snake_case. The exceptions
are `agent.avatar_read` and `agent.set_avatar`, which are camelCase (`agentIds`,
`agentId`, `type`, `seedOnly`), and the `callerHarness` and `callerSession`
fields on `set_labels`, `set_avatar` and `set_github_identity`.

Replies are built as JSON values, not structs. Most return `{"agent": digest}`.
The exceptions:

- `resolve` returns `{"gone": ...}` for a gone agent;
- `list` returns `agents`, plus `gone` and `next_cursor` when present;
- `avatar_read` returns `{"avatars": [...]}`;
- `set_avatar` returns `{"avatar", "applied"}`;
- `github_identity` and `set_github_identity` return `github_identity`;
- `dispose` returns `gone` and `bounced_deliveries`;
- `merge` returns `gone`.

`agent.list` maps its wire `cursor` to the store's `after_agent_id`, defaults
`limit` to 50 and bounds it to 1–200 (`3101-3169`).

The caller fields (`callerHarness`, `callerSession`, and `avatar_read`'s
`harness`, `session_id`, `session`, `caller_directory` and `caller_session`)
are core's residence checks, not identity.

`GithubIdentity` (`agent_registry.rs:600-628`) is tagged by `kind`, snake_case,
and `deny_unknown_fields`. It has two variants:

- `app`: `app_id`, `app_slug`, `installation_id?`, `client_id?`,
  `credential_ref`, `coauthor_line`;
- `user_token`: `login`, `credential_ref`, `coauthor_line?`.

Neither variant holds a token or key. `credential_ref` only names a credential
held elsewhere.

## Avatar fingerprint test vector

`crates/prefrontal-core-module/tests/it/agent_fleet_overview.rs:559-582` pins one
value. The input is genome `"a"` repeated 2048 times, type
`"creature.classic"` and version `2`, and the expected fingerprint is
`b4c04a5d931e65a48e022b8943678d55`. The recipe
(`fleet_overview.rs:96-109`) is BLAKE3 over
`genome + "\n" + type-or-empty + "\n" + decimal-version-or-empty`, keeping the
first 32 lowercase hex characters.

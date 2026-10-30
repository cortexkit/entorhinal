# User profile v1

These samples are regenerated from the real served `ProjectsHandler` on every
run of `user_profile_published_vectors_regenerate_real_handler_bytes`. They use
a scratch store, fixed incarnation `0123456789abcdef` (the identifier for one
module-process lifetime), fixed clock `700`, and
made-up names, expertise, workspace and project ids. Setup registers two
projects in `example-studio`; calls run sequentially as the
`reserved:prefrontal-core` principal recorded for the route, without operator prompts.

Each JSON file contains `op`, `request`, `principal`, and `reply`. For successful
calls, `reply.reply_bytes` is the exact UTF-8 handler response, including its
`result` envelope, not a parsed/re-serialized reply. Refusals are handler
outcomes recorded as `reply.refusal` (`code`, `message`, `detail`), not protocol
response bytes. The enclosing sample JSON is pretty-printed with a final LF.
No real personal data or machine paths appear in these files.

In sequence:

1. `get-empty.json`: empty user object and expertise list; no defaults or nulls.
2. `set-name.json`, `set-address-as.json`, `set-pronouns.json`,
   `set-computer-savviness.json`, `set-verbosity.json`: set all five global fields.
3. `set-workspace-expertise.json`, `set-project-expertise.json`: set both scopes.
4. `get-project-override.json`: project expertise overrides workspace expertise.
5. `get-workspace-fallback.json`: the other project inherits its workspace line.
6. `get-unchanged.json`: the preceding project-context token returns only token
   and `unchanged:true`.
7. `clear-name.json`: clears a field and omits it from the user object.
8. `refuse-invalid-value.json`, `refuse-newline.json`,
   `refuse-unknown-scope.json`, `refuse-implicit-project.json`,
   `refuse-global-scope.json`, `refuse-request-key-reused.json`: the principal
   value, text, scope, shape and request-key refusals.

`SHA256SUMS` lists the SHA-256 of each JSON file. The byte-compare test checks
both regenerated samples and sums. To intentionally regenerate only this set:

```sh
UPDATE_USER_PROFILE_VECTORS=1 cargo test --workspace --all-targets --locked user_profile_published_vectors_regenerate_real_handler_bytes
```

Tokens are opaque state digests, not journal positions. A token from a
project-context read includes effective expertise as well as the whole profile;
the token is meaningful only for that same request context (the same
`project_id`, or none for an unscoped read). Direct confirmation and
other admission refusals are covered by handler tests, not these relay samples.
Callosum excludes writes from its simulator profile in that profile's own
allowed-operation list, rather than in entorhinal admission.

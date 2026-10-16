# Package catalog storage

`ck-catalog` is another argv0 face of `ck-entorhinal`, like `ck-projects` and
`ck-agents`. Placement must provide the `ck-catalog` link to expose `ck catalog`.
The module configures registry trust, verifies packages, walks successor chains,
ingests status feeds, and holds local publisher grants. The core store also holds
shared agent pins and local plans/generations. Agent tools prepare verified skill
closures, obtain user consent and stage generations for consumer prefetch and
core's activation acknowledgement. Background status polling protects held skill
generations. Agent moves carry pinned package bytes through entorhinal's own
`agent-data/v1` participant. Direct operators can publish packages; publishing
remains unavailable until the vault's signing key is provisioned. Non-skill
installation is not exposed yet.

```
ck catalog registry add https://registry.example --scope @example --key-id status-1 --public-key <64-lowercase-hex>
ck catalog registry add https://fallback.example --default --key-id status-1 --public-key <64-lowercase-hex>
ck catalog registry repin https://registry.example --key-id status-2 --public-key <64-lowercase-hex> [--valid-from 2026-11-01T00:00:00Z]
ck catalog registry list [--json]
ck catalog registry remove https://registry.example
ck catalog publisher trust <fingerprint> --agent <agent_id>
ck catalog publish <package dir> --registry <url> --scope <scope>
```

## Offline starter bundle

`ck catalog seed <bundle file>` accepts only an operator-opened Direct route,
not another module or a flow, and needs no Touch ID: it grants no authority.
Entorhinal verifies the entire strict `ck-starter-bundle/v1` ustar,
its root-certified signer, package records and archive bytes before publishing
archives and their embedded `ck.json` through the content-addressed `BlobStore`.
It creates no pins, grants, generations or journal entries. Repeating the same
bundle is idempotent. A bundle must be signed by a publishing key certified by
the compiled `CORTEXKIT_ROOT`. TEST-ONLY roots are injected only by tests.

The signed bundle document binds each exact record/status member through
`record_member_sha256`, checked before parsing. Bundle-attested initial statuses
use the same SQLite compare-and-update boundary as online statuses; a later feed
still needs a pinned registry status key and refuses lower sequences or equal
sequences with different bytes as `feed_rollback`. The bundle pins no registry
key. Migration 11 is amended in place: starter statuses may precede registry
configuration, and a machine-local metadata proof re-authenticates them after
restart. Archives and manifests remain outside SQLite.

At startup, `<store folder>/catalog/starter-bundle.tar` is considered only when
the content store is empty. Loading runs on a background blocking worker, never
on startup or health. A private machine-local `catalog/starter-bundle.attempt.json`
records the attempt and its named outcome, including refusals, so a refused or
interrupted automatic load is not retried at every restart. An explicit seed
command is still allowed after an automatic refusal. The installer owns placing
the bundle file; seeding does not create an agent or activate its packages.

## Publishing

`catalog.publish` accepts the CLI-canonicalized absolute package directory, an
allowlisted HTTPS registry URL, and the package's canonical `@scope`. Only a
Direct operator route can invoke it; flow-scoped routes and other modules cannot.
Packing uses the pure crate's deterministic ustar writer: `ck.json` first, then
regular files ordered by path, with fixed metadata. Links and special files are
refused. Local caps, archive/manifest shape, scope, exhaustive file digests and
flow identity are checked before the operator prompt or any vault call. Checks
requiring registry state (scope grants, version reservations and quotas) remain
the registry's responsibility.

The Touch ID prompt names the package, version, kind and complete archive SHA-256.
A summary over 200 characters is refused, never truncated. Approval gates signing,
not just upload. The published `cortexkit-claustrum-client` opens the vault route
as `reserved:entorhinal` with the supervised launch identity. It reads public key
metadata and signs the pure crate's `ck-record/v1` domain-separated JCS input with
`signing:cortexkit-publish:1`. Entorhinal never requests or stores a private key.
The client owns bounded transient/transport retries; an audit `store_error` is
never accepted as a signature. Key-id mismatch or strict signature failure refuses
before storage or upload.

Signed records are fsynced and atomically persisted under
`catalog/publishes/<request digest>.json` in the local store folder **before** the
first upload. The internal request digest binds registry URL, scope and archive
SHA-256; it is not a new HTTP field or CLI flag. Every retry re-packs the directory
and checks the cached record/signature against its current bytes and vault public
key. Identical bytes reuse the signature after a lost upload reply, including a
module restart. Changed package bytes get a different request digest and require
fresh signing. Publish calls are serialized within the module.

Upload uses the existing allowlisted registry client, sending `PUT` archive bytes,
`application/x-tar`, base64url-unpadded JCS in `CK-Record`, and lowercase hex in
`CK-Signature`. Only 307/308 redirects may preserve the upload, and they must remain
on the original origin and within the configured base path. Replies are bounded
and decoded as the contract's known publish fields, never passed through raw. A
malformed or mismatched success reply is `invalid_reply` with outcome unknown.
The reply carries the package version's first signed status (sequence 1, state
`available`). It must match the upload: the same name, version, archive digest and
publisher fingerprint. Its signature is authenticated with the configured registry
status-key chain.
`publish_unavailable` carries Retry-After advice; repeat the identical command to
resend the durable bytes (there is no internal upload retry or real sleep).
An existing version with different bytes is reported as `version_exists`.
Registry-identical publication is idempotent and returns its original reply.

A scope maps to exactly one registry. An explicit `@scope` mapping wins over the
single optional default; otherwise an unmapped scope is refused. Remapping means
remove, then add. Registry URLs must be HTTPS, without credentials, query, or
fragment; URL parser canonicalization and a trailing-slash normalization identify
an entry. Removal atomically clears its mappings, key chain, feed and status
metadata, not its content-addressed blobs or publisher grant history.
Each time a registry is added, its entry receives a hidden random identifier,
called the entry's incarnation. The incarnation distinguishes the entry from an
earlier, removed entry even when URL, key and confirmation clock are identical.
In-flight requests carry the old identifier and cannot write to the
replacement. Repin preserves the identifier; registry list omits it.

Writes are Direct-only, refused on flow routes, and require the daemon's operator
confirmation on the asynchronous serving path. Prompts identify the URL, key id,
and SHA-256 of the raw public key. Inputs whose complete prompt exceeds 200
characters are refused, including enough room for a later remove prompt. Repin
appends a new key; an id or public key already in that registry's chain is
`key_conflict`. Only the key supplied when the registry is added has a null
`valid_from`. Repin defaults to the
module clock at confirmation, truncated to a whole second. Explicit times must
be real RFC 3339 UTC whole-second `Z` values. Retired keys remain in chain order;
the pure verifier treats their successor's activation instant as an exclusive
retirement boundary.

Migration 11 reserves seven machine-local tables for registries, scope mappings,
key chains, feeds, held status (including signer index), publisher grants (with
retained revocation time), and audit. These tables never join the identity
journal, shared state comparison, identity-change feed, or rebuild projections.
There is no SQLite blob column or blob index.

## Trust and durable status

The chain walker fetches `/v1/registry/keys`, authenticates successors under the
newest held key, and stores each statement's `valid_from`. Every served link that
names a held identity is checked, including operator-pinned anchors without a
statement. An authenticated prefix survives a refused later link; refusal stops
the walk. Historical links before an operator's anchor cannot extend trust.
An unknown status signer triggers one chain fetch before verification is retried.
A replacement lacking the newest held key's signature needs an operator repin.
Bare operator-pinned anchors retain only their pinned identity; received metadata
cannot become an authenticated statement without its predecessor's signature.
Registry removal refuses with `registry_has_status` while any accepted status is
held, even for an unpinned version: removing its signer chain would discard history
that protects against rollback. Use `ck catalog registry repin` to rotate the
registry's key. Registries with no held statuses can still be removed.

Publisher keys from `/v1/scopes/{scope}` are used only for that scope at its
selected registry; another registry's same-named key id cannot supply trust. Historical rotations
check both signatures and lineage, ignoring a predecessor's retirement window
and ignoring revocations whose compromise time is later than the successor's
activation. Invalid rotations and their descendants are not trusted. Publisher
windows remain half-open and are checked against the status-attested publication
time. `CORTEXKIT_ROOT` is the compiled public key of the offline CortexKit root;
test-only roots are supplied explicitly, never installed in production trust.

`verify_version` binds the scope and signed name/version to the request before
accepting status. Held archive digests, publisher fingerprints and publication
times cannot change. The authenticated package status is stored independently of
package manifests and archive blobs: an unavailable or tombstoned archive cannot
erase an authenticated status.
Restoration authenticates held bytes and their signer index and compares every
denormalized column; later repins do not retroactively invalidate saved status
records whose signature was accepted before the signer's retirement.
Package shape, signature and publisher trust pass before package-byte requests.
Tombstoned and blocking-revoked versions refuse before manifest or archive
requests, with `version_tombstoned` or `version_revoked` and the signed reason.
Withdrawn and non-blocking revoked versions remain fetchable so consumers can
read manifests for package versions already pinned to an agent. A withdrawn
version is no longer a new resolution candidate; a non-blocking revocation has a
compromise time later than the version's authenticated publication time.
Starter bundles initialize missing holds only; they never replace existing status
or advance its sequence. Registry updates from a starter hold still obey the
normal sequence, timestamp and terminal-state rules.

Feeds use the same binding boundary for held and previously unheld versions,
without requesting packages. Polls refresh the registry chain. The cursor and
strong ETag advance only when every record applies or is an authenticated
duplicate. Partial acceptance keeps good records, but not the new cursor or ETag.
An incremental feed request falls back to the full feed when the registry's
incremental history has expired, or when an incremental record is refused as a
rollback (`feed_rollback`). An HTTP 304 Not Modified reply changes nothing.
A refused key link stops that scope's poll before the status request: the current
signing epoch may be unresolved, so an older key cannot be assumed current. The
accepted prefix and all previously authenticated status remain held for retry
after chain repair. A refused link alone does not invent a revocation or degrade
active generations; only authenticated blocking status does so.
Status writes reapply against current mapping, chain and holds inside the SQLite
transaction. Verification rechecks again after downloads before publishing both
the verified manifest and archive within that fence.
An in-flight request cannot overwrite newer version status or attach old
signatures to a replacement registry's key chain.

Blob publication is not a cross-resource transaction: status is already durable,
and verified bytes are atomically renamed while the final status check is fenced.
A storage failure keeps accepted status. A crash after rename may leave a verified
orphan blob; every read still hashes the whole blob. An interrupted temporary file
is not addressable as a blob, and no partial or unverified bytes become servable.

## Publisher grants

Publisher trust is Direct-only, forbidden on flow routes, and asks for operator
confirmation naming **install**, the publisher fingerprint and the placement
agent. It authorizes install only, not future upgrades. The ceiling is the sorted
union of authenticated manifests in that agent's pins. A held archive supplies
`ck.json`; otherwise the selected registry supplies the bounded manifest bytes.
Their SHA-256 and strict manifest name/version must match the pin. Any failure
returns `manifest_unavailable` with the package identity, without writing a grant.
Production uses the agent's stored authenticated pins (`StoredPins`), filtered
by publisher fingerprint. An absent authenticated pin set refuses rather than
minting an empty capability ceiling. The Touch ID summary includes the complete
granted capability ceiling (or `none`) when the identity and tokens fit within
200 characters. Otherwise it retains the full publisher and placement identity:
an unbounded token list cannot fit without truncating the authorization facts.

A grant's ceiling is the list of capability tokens it allows. An empty ceiling
covers packages that need no capability tokens. Coverage otherwise requires an
unrevoked grant row for the package's publisher fingerprint, the operation and the
placement agent, capabilities within its ceiling, and no scripts.
Root-certified CortexKit packages never need publisher grants; their pins cannot
be used to mint one. A different recovered publisher key inherits no grants.
Refresh preserves the row id, unions ceilings, replaces time/card reference and
clears revocation. Revocation retains history. Insert, refresh and revoke append
`grant_insert`, `grant_refresh` and `grant_revoke` audit rows atomically, including
the acting principal. `permissions/v1` lists them under `catalog_publisher` and
revokes them; disposed agents and revoked rows are hidden. These rows never travel
through the shared log. `agent.audit_list` does not expose catalog audit rows.

## Transport and blobs

The registry client uses reqwest with default features disabled and rustls's
bundled WebPKI roots. It never uses a native TLS provider or system proxy.
Production construction accepts HTTPS only; loopback HTTP construction exists
only in tests. Every request, including every redirect, is checked against the
configured registry origins and base-path boundaries. Ten redirects are allowed.
Scope selection is explicit mapping first, then default. Client limits are
constructor inputs; module defaults are 10 seconds to connect (including TLS),
120 seconds total for archives, 30 seconds for JSON and manifests, 10,485,760
archive bytes, 65,536 manifest bytes, and 8,388,608 JSON bytes. The streamed byte
limit applies even without Content-Length. The total deadline spans all redirects
and body chunks. Archive reading and digest checks use `entorhinal-catalog`, not
another tar implementation.

`publish_unavailable`, `status_key_unavailable`, and `status_unavailable` HTTP 503 refusals are typed
`NetworkError::Retryable` values, carrying the registry refusal kind and parsed
`Retry-After` advice. The parsed `Retry-After` delay, whether given as
delta-seconds or an HTTP date, is capped at one hour;
a missing header uses the named `DefaultMissing` policy and an invalid header
uses `DefaultInvalid`, both defaulting to 60 seconds. Past HTTP dates give zero
delay. Error response bodies use the JSON reply cap. The client never retries
these refusals internally: the caller chooses whether and when to repeat the
same request unchanged. For withdraw and unwithdraw, `status_unavailable` means
nothing committed, so the identical `after_sequence` compare-and-set can be
retried. A lost race remains the non-retryable HTTP 409 `status_sequence_conflict`,
even when a `Retry-After` header is present. A 410 with `feed_after_unavailable` is separately typed
as `NetworkError::FeedAfterUnavailable`, so a feed caller can request the full
feed when the registry's 30-day incremental history has expired. This transport
does not ingest feeds or perform that fallback internally.

Blobs are `catalog/blobs/<sha256>` beside `store.db`, in private `0700`
directories. Publication uses a private exclusive temporary file, file fsync,
rename, and directory fsync on Unix. Existing blobs are reused only after a
successful digest check. Failed pre-rename publication cleans up its temporary
file. Temporary filenames cannot be requested as blobs. Every read hashes the
whole bounded file and serves a slice from exactly those verified bytes; no
package data is written to SQLite or to the identity journal.

`catalog.read_blob {sha256, offset, length}` is asynchronous and requires a
`reserved:<module>` caller whose current daemon catalog entry declares the exact
one of `ck-catalog-consumer-skill/v1`, `ck-catalog-consumer-persona/v1`,
`ck-catalog-consumer-flow/v1`, or `ck-catalog-consumer-memory-profile/v1` in
`provides`. There is no generic consumer capability. It rechecks the daemon catalog
on every call rather than caching a possibly stale admission. `length` must be
1..=1,048,576, the range cannot overflow, and offset must not exceed the total
length. The response is `{bytes: <standard-base64>, offset, total}`. Reads clip at
EOF and are empty only there. Refusals include `catalog_consumer_required`,
`blob_request_invalid`, `blob_unknown`, `blob_corrupt`, and storage errors.

## Verification boundary

Loopback tests serve unchanged signed test documents and archives on
contract HTTP paths, plus programmable redirects and barriers. No new private
signing keys are introduced. Clock-sensitive tests advance Tokio's paused clock;
loopback socket tests keep that clock fixed while OS I/O completes. The pure
crate's archive failure corpus is also fetched through the transport.

Whether macOS shows its network permission prompt for a new user account remains
a manual deployment check; Linux CI cannot observe it. The dependency inventory
below records the additional lockfile packages introduced by the transport and
base64 encoding.

## Dependency inventory

Trust tests add `ed25519-dalek 2.2.0` as a module dev-dependency only, at the
already locked version. No package versions or runtime dependencies are added.
Extra signed HTTP replies use only published TEST-ONLY signing keys (test keys
whose private seeds are public) from
`crates/entorhinal-catalog/tests/vectors/ck-catalog-v1/statements.json`; those
keys gain no trust merely because tests generated a signature.

New direct module dependencies: `entorhinal-catalog` (workspace path),
`reqwest 0.12.28` (default features off, `rustls-tls`), `base64 0.22.1`,
and already-locked `chrono 0.4.45` (default features off, `std` only).
Existing locked package versions were preserved. Packages newly resolved in the
lockfile for these transport dependencies, including optional and non-Linux
target dependencies:

- `atomic-waker 1.1.2`
- `base64 0.22.1`
- `base64 0.23.1`
- `bumpalo 3.20.3`
- `cfg_aliases 0.2.2`
- `chacha20 0.10.2`
- `form_urlencoded 1.2.2`
- `futures-channel 0.3.34`
- `getrandom 0.4.3`
- `http 1.5.0`
- `http-body 1.1.0`
- `http-body-util 0.1.5`
- `httparse 1.10.1`
- `hyper 1.12.0`
- `hyper-rustls 0.27.10`
- `hyper-util 0.1.21`
- `icu_collections 2.3.0`
- `icu_locale_core 2.3.0`
- `icu_normalizer 2.3.0`
- `icu_normalizer_data 2.3.0`
- `icu_properties 2.3.0`
- `icu_properties_data 2.3.0`
- `icu_provider 2.3.1`
- `idna 1.1.0`
- `idna_adapter 1.2.2`
- `ipnet 2.12.2`
- `js-sys 0.3.106`
- `litemap 0.8.3`
- `lru-slab 0.1.3`
- `percent-encoding 2.3.2`
- `potential_utf 0.1.6`
- `quinn 0.11.12`
- `quinn-proto 0.11.19`
- `quinn-udp 0.5.16`
- `r-efi 6.0.0`
- `rand 0.10.3`
- `rand_core 0.10.1`
- `rand_pcg 0.10.2`
- `reqwest 0.12.28`
- `ring 0.17.14`
- `rustc-hash 2.1.3`
- `rustls 0.23.46`
- `rustls-pki-types 1.15.1`
- `rustls-webpki 0.103.15`
- `rustversion 1.0.23`
- `ryu 1.0.23`
- `serde_urlencoded 0.7.1`
- `sync_wrapper 1.0.2`
- `thiserror 2.0.21`
- `thiserror-impl 2.0.21`
- `tinystr 0.8.4`
- `tinyvec 1.13.3`
- `tokio-rustls 0.26.6`
- `tower 0.5.3`
- `tower-http 0.6.11`
- `tower-layer 0.3.3`
- `tower-service 0.3.3`
- `try-lock 0.2.5`
- `untrusted 0.9.0`
- `url 2.5.8`
- `want 0.3.2`
- `wasm-bindgen 0.2.129`
- `wasm-bindgen-futures 0.4.79`
- `wasm-bindgen-macro 0.2.129`
- `wasm-bindgen-macro-support 0.2.129`
- `wasm-bindgen-shared 0.2.129`
- `web-sys 0.3.106`
- `web-time 1.1.0`
- `webpki-roots 1.0.9`
- `windows-sys 0.52.0`
- `windows-targets 0.52.6`
- `windows_aarch64_gnullvm 0.52.6`
- `windows_aarch64_msvc 0.52.6`
- `windows_i686_gnu 0.52.6`
- `windows_i686_gnullvm 0.52.6`
- `windows_i686_msvc 0.52.6`
- `windows_x86_64_gnu 0.52.6`
- `windows_x86_64_gnullvm 0.52.6`
- `windows_x86_64_msvc 0.52.6`
- `writeable 0.6.4`
- `yoke 0.8.3`
- `yoke-derive 0.8.4`
- `zerotrie 0.2.5`
- `zerovec 0.11.8`
- `zerovec-derive 0.11.6`

## Agent pin sets and local execution state

Migration 12 adds `catalog_pin_set` (one stored `pin_digest` per agent) and
`catalog_pin` (one row per agent/name). Pins contain name, version, kind, manifest
and archive SHA-256, publisher fingerprint, `root` and `range`. `root` marks a
package named directly in an install or upgrade request rather than reached only
as a dependency. Its `range` records the requested semver range; transitive pins use
`root=false` and `range=null`. Pins contain no machine-local generation id or
publisher trust fact. An empty set has the digest of the empty JCS array.

The digest is **lowercase SHA-256 of a JCS array sorted by name**. Each object has
exactly `{name, version, manifest_sha256, archive_sha256,
publisher_fingerprint, root, range}`; `root` is a boolean and `range` is a string
or null. Agent id and kind are excluded. A fixed two-pin golden pins this encoding
and proves that changing root metadata changes the digest. One core validator and
one digest function serve local writes, remote apply, snapshots, verify and rebuild.

`catalog.pin` replaces one complete set, with shared entry shape
`{op: "catalog.pin", agent_id, pins, pin_digest}`. With the identity log enabled,
the module records a pending log append before committing its journal and pin
projection together, so an uncertain network reply can be retried with the same
bytes. This internal write helper adds no public pin RPC. Remote changes require an existing live agent,
unique canonical names, valid versions/ranges and lowercase 64-hex digests.
Snapshots may retain pins of terminal agents for identity history. Generic
`project.shared` changes cannot write pin tables. Pins participate in shared
snapshots and journal replay, but never in `IDENTITY_CHANGE_OPS` or `agent.changes`.

`catalog_plan`, `catalog_generation` and `catalog_transition` are machine-local,
not journal projections. `catalog_generation.publisher_facts` records the package
scope and the install-time first-party certificate result beside each generation;
this is deliberately not copied with shared pins. Plan creation is self-targeted,
live-agent-only, and accepts install/upgrade for the calling agent, not `hire` (creating another agent)
or workspace/project-wide placement targets. A ready closure containing
any non-skill member becomes failed with `kind_not_yet_supported` and its package
name. Plans start pending and become ready or failed. Approval of a ready plan
creates one approved generation; installed is reserved for consumer bookkeeping.
Readiness, activation and degradation are separate store transactions. Approval
checks the base pin digest and held closure status snapshot (`plan_stale`); changes
outside the closure do not stale it. Revoked/tombstoned closure members block
activation. The agent approval coordinator uses the shared pin writer in the same
transaction as the local generation, consent receipt and optional publisher grant.
When sharing is enabled, all these writes wait for the identity-log append to be
confirmed; a refusal rolls them all back. Journal authorization evidence is checked
on replay, not interpreted as fresh authority on another machine.

Every applied plan or generation transition (plan create, ready and failed;
generation approve, ready, active and degraded) records its original result and
argument digest locally.
Repeats return that result, even after a later transition; reuse with different
arguments is `request_key_conflict`. A partial unique index permits at most one
active generation per agent. Activation supersedes the previous active (including
a degraded former active) and activates the ready replacement in one transaction.
The old row and closure remain available while consumers finish switching away
from the old packages. **Superseded is local bookkeeping,
never a catalog wire state**; generation reads expose only the contract's
installed/approved/ready/active/degraded states.

## Agent tools and consumer protocol

These tools are on `tool-provider/v1`, not management aliases. Every agent role
may install only for its own daemon-stamped identity. Worker scopes, flow scopes,
unowned scopes, nondelegating runs, terminal agents and claims held elsewhere are
refused by the existing agent-tool admission rules. Caller/role arguments grant
no authority.

- `catalog_prepare {spec, request_key}` accepts
  `spec: {operation: "install" | "upgrade", target: {agent_id}, package, range}`.
  The target must equal the stamped caller; hire and tier installs refuse
  `operation_not_yet_supported`. Invalid names and ranges fail before a job starts.
  The result is `{plan_id, state:"pending"}` without waiting for any network work.
  The same agent/request key and spec return the same plan; another spec is
  `request_key_conflict`. Repeating a pending prepare after restart resumes it.
- The background job refreshes every involved scope's authenticated status feed,
  enumerates bounded candidates, verifies records/manifests/archives through
  `verify_version`, publishes verified archives to the blob store, and runs the
  pure local resolver. It never holds a store write lock across network requests.
  There are at most 256 package names and 1024 verified candidate versions per job.
- `catalog_plan {plan_id}` returns pending, failed with a named refusal, or ready
  with the exact closure, publisher trust, moves, consumer checks and composite card.
  Migration 12 stores the verified metadata, capability ceilings, moves and checked
  consumer sections in machine-local `catalog_prepared`, outside replay/export.
- `catalog_install {plan_id}` accepts only ready plans; otherwise it
  refuses `plan_not_ready` or `plan_failed`. Idempotence is **per plan_id**; no
  additional request key is accepted. Approval binds exact closure digests,
  the base pin digest and the consumer text. Both base pins and held closure status
  are revalidated inside the shared writer transaction (`plan_stale`).
- `catalog_generation {}` reads the caller's current and pending local generations.
  It never exposes the old `superseded` bookkeeping rows.

Consumer discovery reads the daemon's supervisor list and catalog, then opens the
enabled, ready modules' `catalog-consumer` internal service. Each of the four kind
capabilities above declares exactly that kind; no marker means no ownership of
that kind. All declaring consumers of a needed kind must participate. Missing,
unready or unroutable consumers refuse `consumer_unavailable` with the module named
(skills require prefrontal-core when no skill consumer is declared).

Alongside each kind capability (the `ck-catalog-consumer-<kind>/v1` declaration
that claims a package kind), a consumer must declare in its manifest's
`provides` list this exact `internal_service` provider role for the
`catalog-consumer` service:

```json
{"role":"internal_service","service_id":"catalog-consumer","transport":"bulk","agent_facing":false,"operations":["catalog_consumer.check","catalog_consumer.prefetch"]}
```

The service id has no identifier grammar and is compared by exact bytes. Entorhinal
uses one `catalog_consumer::SERVICE` constant for routing and fake consumer roles.
A kind capability without the role cannot open a route: preparation fails, or an
approved generation stays held, with `consumer_unavailable`, the module id and
the daemon's original route refusal code/detail. Missing roles never imply success.

- `catalog_consumer.check {plan_id, packages}` receives only that consumer's kinds,
  with exact pin identities, publisher fingerprint/trust and verified capabilities.
  It returns `{ok:true, card_section:"human text"}` or
  `{ok:false, refusals:[{package, version, code, message}]}`. A non-string or blank
  section is `consumer_contract_violation`, naming the module. Consumers read bytes
  through `catalog.read_blob` and independently verify them.
- `catalog.read_blob {sha256, offset, length}` is available only to a daemon-stamped
  module that declares a catalog-consumer kind capability. It returns
  `{bytes, offset, total}`, where `bytes` is standard base64; requested `length` is
  `1..=1,048,576`, and a valid request is clipped at EOF. An empty `bytes` value is
  returned only when `offset` is exactly the blob length. Refusals include
  `catalog_consumer_required`, `blob_request_invalid`, `blob_unknown`, `blob_corrupt`
  and `blob_io` (malformed request fields are `invalid_params`). Fetch `ck.json` by
  its `manifest_sha256` and the archive by its `archive_sha256`; consumers must
  verify the returned bytes independently.
- `catalog_consumer.prefetch {generation_id, packages}` returns `{ready:true}` only
  after durable caching and verification. Every checked consumer must still be
  present with its checked kinds, and every current covering consumer must report
  ready. Otherwise the generation stays approved with a named error; retrying the
  install retries prefetch without another card or pin write.

The composite consent card is the one approval request the operator sees for an
install. It combines every participating consumer's card section into a single
generic-consent request to Cingulate, the module that shows consent prompts to
the operator: one fact per consumer, sorted by module id,
with a human label such as `Skills (prefrontal-core)` and exactly the
consumer's section text. The exact `consent.request` method/params envelope is
bounded to 1,310,720 bytes at prepare and again before requesting consent. An
oversized card fails `card_too_large`, naming its consumers; nothing is truncated.
No separate fact-count or field-length ceilings are invented.

Options are `approve_once`, `approve_always_publisher` and `deny`. Always-publisher
is offered only for installs, never for a closure containing root-certified
`@cortexkit` packages. It records one install-only, self-placement grant per
publisher, with that publisher's union of verified capability tokens as the ceiling.
Coverage is evaluated by the shared `coverage()` function, including mandatory
approval for scripts, and rechecked before commit. Fully covered closures install
without a card. Consent settlements are recorded before acknowledgement using the
existing consent coordinator; terminal retries replay the committed outcome once.

### Local capability declarations

`LocalCapabilities` supplies the resolver's local `cap/<area>[.<verb>]@<major>`
tokens and remains injectable for tests. The production source reads the daemon
catalog and unions the declarations of **ready** modules. Names beginning `cap-`
pass through the pure crate's `manifest_to_capability` adapter: `cap-code-read/v1`
becomes `cap/code.read@1`, and `cap-shell/v1` becomes `cap/shell@1`. Malformed names
are ignored and logged once per distinct name, never accepted as tokens. Missing
capabilities still fail the resolver's normal eligibility path, naming the package
and token. No new consumer operation or ad hoc spelling conversion is introduced.

### Core activation operations

`catalog.agent_generation {agent_id}` and
`catalog.ack_activation {agent_id, generation_id}` accept calls only from the
`reserved:prefrontal-core` route, never from Direct or a flow-scoped route. The read returns
`{active, ready}` with closure digests and `pin_digest`; ready includes only newer
generations. Acknowledgement is idempotent and refuses a generation that is not
ready or whose held status blocks activation. Both operations use local state
only: no registry, daemon discovery or consumer request runs on this path. Core
retains its last active generation when Entorhinal is unavailable at session start.

All three generation surfaces (`catalog_generation`, `catalog.agent_generation`,
and a successful `catalog.ack_activation`) use one generation serializer. Each
closure entry adds `publisher: {fingerprint, scope, first_party}` while retaining
`publisher_fingerprint`. `scope` is the installed package scope (for example,
`@cortexkit`); `first_party` is the root-certificate result recorded for that local
generation at install time. Publisher grants never make a package first-party.
The fact is machine-local: shared pins do not carry it, and a Mac receiving a move
must certify CortexKit keys against its own root. The generation optionally includes
`pinned_status: [{name, version, state, reason}]`, listing only closure versions
whose held status is not `available`. The field is **absent**, not an empty list,
when there is no status news. Pin identities and digests do not change.

For a **withdrawn** pin, core sees the same `active` (or `ready`) generation, its
nonblocking `pinned_status` notice, and no new error. Existing pins remain usable;
withdrawal does not degrade a generation or block acknowledgement. For an applicable
**revoked** pin, core sees its active generation become `degraded`, with
`error: {code: "pinned_revoked", name, version, reason}` and the revoked pin in
`pinned_status`. Tombstones have the same blocking effect with `pinned_tombstoned`.
Core can show the blocking notice and omit the affected skill on its next render;
Entorhinal does not rewrite a running session's rendered files. A ready generation
remains ready but cannot be activated: acknowledgement refuses `activation_blocked`
with `detail: {name, version, reason}`. Even a retry of an old successful ack cannot
replay past newly learned blocking status. Prepare fails with `version_revoked` or
`version_tombstoned`, naming the blocked version, rather than silently reusing it.

The revocation test uses only registry-attested `published_at`: at or after
`compromise_from` is blocked, strictly earlier remains usable, and an unknown
publication time is blocked. A tombstone is unconditional. Status ingestion and
its effects on active skill generations commit in the **same SQLite transaction**,
whether the status came from a background feed or version verification during
prepare. Failed effects roll back status, feed cursor and ETag together. Health
and core generation reads never need HTTP.

The byte-exact reserved-core examples are in
[`test-vectors/catalog-generation-v1`](../test-vectors/catalog-generation-v1/README.md).

### Background status polling

After module startup opens its local store, one background task polls each distinct
scope in the shared local pin set, including with a default registry mapping. Each
poll refreshes registry keys and reuses the status-feed verifier, held ETag and
`?after=` cursor. Existing full-feed recovery rules handle history older than 30
days or feed rollback. A 304 changes no held status, cursor, ETag or generation.
Prepare retains its existing immediate refresh for requested and pinned scopes;
it does not enqueue a duplicate background pass.

The hourly phase is a random millisecond offset in `[0, 3600000)`, saved once in
machine-local `catalog_poll_clock` storage, not the shared identity journal. Startup
polls immediately; subsequent successful passes align to that wall-clock phase.
One failed pass backs off for 2 hours, then 4, then 6 (capped), measured by Tokio's
monotonic timer. A successful pass resets failures and returns to the next hourly
phase. Scopes are rediscovered each pass; one scope's failure does not skip others.
The poller holds only a weak store slot, and opens transient store handles solely
for synchronous database work, never across an HTTP await.

Health reads only atomic gauges: `catalogLastSuccessMs` (null before a successful
pass), `catalogLastError` (null or a named code), `catalogConsecutiveFailures`
(failed passes since the last successful pass), and `catalogScopesPolled`
(cumulative scope attempts). A pass with no pinned scopes is successful local work.
The last error is cleared on success. These gauges do not independently change the
module's health status and do not include the foreground prepare refresh.

Implementation decisions where the CortexKit catalog contract v1 leaves details
open: one schedule and
backoff apply to a whole pass; an empty pinned set is a successful pass; the first
retry is 2 hours; success realigns to the hourly phase rather than retaining retry
drift; the first blocked pin in closure order supplies a generation's single error
while `pinned_status` lists all status news; an already degraded generation updates
its blocking reason on subsequent status news. The local scheduling table is
created lazily for stores predating polling. The offset belongs to this local store
and is never replicated to other Macs through the shared identity journal.

### Integration decisions

The agent-targeted prepare shape resolves the placement-tier ambiguity for this
skill-only implementation; the consumer kind capabilities follow the daemon's
actual grammar rather than the earlier proposed slash-separated markers. Check and
prefetch share one consumer interface, human sections use existing Cingulate fields,
and activation uses the existing reserved-core admission identity. Existing
resolver rules remain authoritative: re-resolved subjects choose the highest
eligible version, unrelated pins remain fixed, replacing a root replaces its range,
and status changes stale a ready plan. Moves, workspace tiers, hire and additional
package kinds remain separate work.

## Multi-root resolver adapter

An agent can have several root packages (packages it installed directly), but
the resolver takes one root. The roots are combined under one synthetic root
whose requirements are a range per package: roots being installed or upgraded
keep their requested range, while every other root, and every pinned package it
reaches through its dependencies, is held at its exact pinned version. The pure
crate's `synthetic_root_ranges(registry, roots, subject_roots, current_pins)`
constructs the root requirements from the pinned versions' own dependency graph.
Core passes root metadata and held-status-overlaid candidates to the pure
`resolve_roots` entry point; there is no second range builder or conflict mapper. Callers supply locally verified
record/manifest candidates (including the manifests of current pins); core overlays
held `catalog_status` eligibility and the caller's local capability tokens. Unheld
versions are not candidates. No network or content-store I/O runs under the store
read snapshot.

- Install makes the named spec the subject root. Non-subject roots and their
  pinned dependencies get exact `=<version>` synthetic-root ranges.
- A named upgrade makes that root and its dependencies not reachable from any
  other root the subject. Its requested range replaces its old range. Packages
  outside that set remain exact pins.
- Upgrade-all uses every root's recorded range, releasing all dependencies.

Subject dependencies are computed from current pinned manifests, not from newer
candidates. Subject dependency packages are not synthetic roots, so a dependency
is removed when no remaining root needs it. The synthetic root is stripped from
closure and moves. The pure resolver computes up/down/same/added/removed against
all current pins. An exact-pin range conflict becomes `pin_conflict`, identifying
the package, `pinned_version`, requesting package and incompatible range from the
resolver refusal; other resolver refusals retain their codes. Install never moves
an unrelated pin, and a named upgrade cannot move a shared dependency.

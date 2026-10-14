# Machine-local package catalog

`ck-catalog` is another argv0 face of `ck-entorhinal`, like `ck-projects` and
`ck-agents`. Placement must provide the `ck-catalog` link to expose `ck catalog`.
This slice configures registry trust and transports bytes; it does not resolve or
install packages, walk successor chains, ingest status feeds, or grant publisher
trust.

```
ck catalog registry add https://registry.example --scope @example --key-id status-1 --public-key <64-lowercase-hex>
ck catalog registry add https://fallback.example --default --key-id status-1 --public-key <64-lowercase-hex>
ck catalog registry repin https://registry.example --key-id status-2 --public-key <64-lowercase-hex> [--valid-from 2026-11-01T00:00:00Z]
ck catalog registry list [--json]
ck catalog registry remove https://registry.example
```

A scope maps to exactly one registry. An explicit `@scope` mapping wins over the
single optional default; otherwise an unmapped scope is refused. Remapping means
remove, then add. Registry URLs must be HTTPS, without credentials, query, or
fragment; URL parser canonicalization and a trailing-slash normalization identify
an entry. Removal atomically clears its mappings, key chain, feed and status
metadata, not its content-addressed blobs or publisher grant history.

Writes are Direct-only, refused on flow routes, and require the daemon's operator
confirmation on the asynchronous serving path. Prompts identify the URL, key id,
and SHA-256 of the raw public key. Inputs whose complete prompt exceeds 200
characters are refused, including enough room for a later remove prompt. Repin
appends a new key; an id or public key already in that registry's chain is
`key_conflict`. Only the add key has a null `valid_from`. Repin defaults to the
module clock at confirmation, truncated to a whole second. Explicit times must
be real RFC 3339 UTC whole-second `Z` values. Retired keys remain in chain order;
the pure verifier treats their successor's activation instant as an exclusive
retirement boundary.

Migration 11 reserves seven machine-local tables for registries, scope mappings,
key chains, feeds, held status (including signer index), publisher grants (with
retained revocation time), and audit. These tables never join the identity
journal, shared state comparison, identity-change feed, or rebuild projections.
There is no SQLite blob column or blob index.

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

`publish_unavailable` and `status_key_unavailable` HTTP 503 refusals are typed
`NetworkError::Retryable` values, carrying the registry refusal kind and parsed
`Retry-After` advice. Delta-seconds and HTTP-date values are capped at one hour;
a missing header uses the named `DefaultMissing` policy and an invalid header
uses `DefaultInvalid`, both defaulting to 60 seconds. Past HTTP dates give zero
delay. Error response bodies use the JSON reply cap. The client never retries
these refusals internally: the caller chooses whether and when to repeat the
same request unchanged. A 410 with `feed_after_unavailable` is separately typed
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
`ck-catalog-consumer/v1` capability in `provides`. It rechecks the daemon catalog
on every call rather than caching a possibly stale admission. `length` must be
1..=1,048,576, the range cannot overflow, and offset must not exceed the total
length. The response is `{bytes: <standard-base64>, offset, total}`. Reads clip at
EOF and are empty only there. Refusals include `catalog_consumer_required`,
`blob_request_invalid`, `blob_unknown`, `blob_corrupt`, and storage errors.

## Verification boundary

Loopback tests serve unchanged frozen signed vector documents and archives on
contract HTTP paths, plus programmable redirects and barriers. No new private
signing keys are introduced. Clock-sensitive tests advance Tokio's paused clock;
loopback socket tests keep that clock fixed while OS I/O completes. The pure
crate's archive failure corpus is also fetched through the transport.

The clean-account macOS network-prompt observation remains a manual deployment
check; Linux CI cannot establish it. The dependency inventory below records the
additional lockfile packages introduced by the transport and base64 encoding.

## Dependency inventory

New direct module dependencies: `entorhinal-catalog` (workspace path),
`reqwest 0.12.28` (default features off, `rustls-tls`), `base64 0.22.1`,
and already-locked `chrono 0.4.45` (default features off, `std` only).
Existing locked package versions were preserved. Additional resolved lockfile
packages, including optional and non-Linux target dependencies:

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

# entorhinal-catalog

A pure Rust implementation of CK catalog v1. The caller supplies bytes, the
scope's trusted publisher keys, registry-attested publication time, pinned
registry status-key chain and held state. This crate has no I/O, runtime,
database or clock, and never extracts or executes a package.

- `names`: ASCII canonicalization, Basal ids, authoritative binding and claims.
- `jcs`: duplicate-refusing JSON parser and RFC 8785 serialization.
- `archive`: strict borrowed-member ustar reader and deterministic writer.
- `record`: seven-stage signed-record verification against exact manifest/archive
  bytes. `Trust.keys` must already be selected for the package's scope.
- `status`: verified held records, monotonic status and feed application. Store
  the returned canonical bytes and signer chain index together; keep chain
  indices stable when appending or explicitly re-pinning registry keys.
- `statements`: publisher rotation, root certificates, registry-key successors,
  half-open key windows and status compare-and-set requests.
- `semver` and `resolver`: the restricted range grammar, accumulated-range BFS,
  deterministic cycle/ref checks, pin moves and unnamed worker selection.

All verification APIs return typed `Result`s. Refusal enums serialize to the
contract's lowercase wire codes. The writer takes ordered `Member`s, requires
`ck.json` first, preserves the remaining order and fixes all metadata. Invalid
paths return `path_invalid`, `path_too_long`, or `path_not_ustar`; member violations
return `archive_invalid` plus a rule; caps return `archive_too_large` and
`manifest_too_large`. The otherwise unnamed non-first-manifest refusal uses
`archive_invalid` with rule `ck_json_not_first`. `MemberKind` makes forbidden
member inputs representable for refusal, without exposing raw header overrides.

The ten pinned vector files are unchanged. Their README provides the six JSON
checksums; integrity tests pin the four auxiliary checksums separately because
a README cannot include its own SHA-256. No production code embeds the public
TEST-ONLY seeds or keys.

## Normative difference from the pinned vectors

Contract §9.1 requires a prerelease range to be an exact singleton comparator
set. The reference generator at `generate.ts:2331-2338` accepts
`1.3.0-beta.1 >=1.0.0` but makes it unsatisfiable. This crate refuses that range;
the resolver reproduction test explicitly asserts the known difference. Every
other vector outcome is asserted structurally. The contract's final status-key
retirement/time/epoch rules and reviewed slice specification extend the older
generator's id-to-key-map status API; additional tests cover those rules.

## Verification

From the workspace root use the worker guide's workspace format, clippy and test
gates. `mutations.toml` contains six catalog controls with native cargo targets;
replay each using `ckdev-mutate run --only <id>`.

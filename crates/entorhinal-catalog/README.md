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

The ten vector files are pinned to prefrontal commit
`610329529`. The README in
`tests/vectors/ck-catalog-v1/` provides the six JSON checksums. Integrity tests
separately pin that README, `SOURCE.txt`, `generate.ts`, and `generate.test.ts`,
because a README cannot include its own SHA-256. No production code embeds the
public TEST-ONLY seeds or keys.

## Signed documents and vector coverage

Signed-document parsing rejects fraction and exponent number tokens and `-0`
before canonicalization or signature verification. General JSON, including
package manifests, retains RFC 8785 number handling. Contract §9.1 and the
vectors both refuse prereleases outside exact singleton comparator sets.
Every vector outcome is asserted structurally. The vector generator models a
registry's status keys as a plain id-to-key map. This crate instead applies the
contract's full key-chain rules (a retired key can't sign records dated after
its successor's `valid_from`, and the signer's position in the chain can't move
backwards), and its own tests cover those rules.

## Verification

From the workspace root, run the gates listed in `CONTRIBUTING.md` (format,
clippy and the workspace tests).

# CK catalog contract v1: reference test vectors

These vectors freeze the byte-level parts of the CK catalog contract v1: the
package-name to Basal-id mapping and scope claims, the signed `ck-record/v1`,
the strict ustar archive reader, the signed `ck-status/v1` and the status
feed, the signed key and status statements, and the resolver. The CK registry
(TypeScript, in cortexkit-cloud) and Entorhinal (Rust) MUST both reproduce
these bytes and outcomes exactly, and each pins this directory at a named
prefrontal commit.

> **TEST-ONLY KEYS.** Every signature here is made with Ed25519 seeds that are
> published on purpose. Each seed is SHA-256 of a fixed sentence printed next
> to it. Anything signed by these keys proves nothing. No client, registry or
> trust store may ever accept them outside these vectors.
>
> Each role has its own keys, so no case passes because two roles share one:
> a TEST-ONLY `@cortexkit` root key, three TEST-ONLY registry status keys,
> two TEST-ONLY `@cortexkit` publish keys and three TEST-ONLY third-party
> publisher keys. `statements.json` → `keys` lists all of them; `record.json`
> and `status.json` list the ones they use.

## Files

| File | What it pins |
| --- | --- |
| `names.json` | The pure name-to-Basal-id function: valid names (short form, exactly 46 bytes, hashed form, two names sharing a 29-byte prefix, prefixes ending on a hyphen or on the separator, ASCII case folding, a 39-byte scope and a 64-byte package name at their limits), refused names with the part that failed (including the R1 boundary-hyphen cases `@a-/b` and `@a/-b` and the 40-byte scope and 65-byte name), the registry's binding check (`id_bound`, `name_not_canonical`), and scope claims (`scope_reserved` for `cortexkit`, `ck`, `catalog`, `registry`). |
| `record.json` | JCS (RFC 8785) cases, two real packages (a skill with a script and a flow) as members, `ck.json` text and ustar archive bytes, their `ck-record/v1` records with canonical JCS text, SHA-256, the exact signing input and the Ed25519 signature, then negative cases with the refusal code each must produce. |
| `archive.json` | The strict ustar reader: one valid archive with its SHA-256 and members, then one case per refusal (bad header checksum, wrong magic or version, base-256 size, missing terminator, non-zero bytes after the terminator, pax and GNU headers, links, directory entries, `..` and other bad paths, a path over 255 bytes, duplicates and case collisions, a truncated member, a missing `ck.json`, more than 1,000 members), each with its expected `archive_invalid` rule and offset; and the pure path rules (`path_invalid`, `path_too_long`, `path_not_ustar`). |
| `status.json` | Signed `ck-status/v1` records for one skill version (published, withdrawn, withdrawal reversed, tombstoned, plus a conflicting second record) and one flow version (published, revoked by key compromise, tombstoned), the client's apply cases (sequence order, `feed_rollback` for a lower sequence or the same sequence with other bytes, terminal states, signature and domain, a publisher-signed record, unknown registry key), the status feed (`feed_seq`, `?after=` replies including `feed_after_unavailable`, and the client's poll outcomes including `feed_rollback` and the full-feed fallback), the compromise-from time rule and the per-kind effect table. |
| `statements.json` | The signed statements: `ck-registry-key/v1` (the registry status key chain), `ck-key-rotation/v1` (both signatures), `ck-key-cert/v1` (signed by the TEST-ONLY root, with key-window cases) and `ck-status-request/v1` (withdraw and unwithdraw with `after_sequence`), each with valid and negative cases. |
| `resolver.json` | The semver dialect (range satisfaction, refused ranges and versions, precedence order), the capability token grammar `cap/<area>.<verb>@<major>`, and closure resolution cases with expected picks, pin moves and refusals. |
| `starter-bundle.json` | A root-certified two-package starter, exact record-member SHA-256 bindings, eight refusal cases, and a standalone persona-manifest case (a body declaring `memory_scope` is refused as `manifest_invalid`). Copied from the prefrontal repository at ff0608080; the other vectors retain the earlier revision named in SOURCE.txt. |
| `generate.ts` | The generator and reference implementations. |
| `generate.test.ts` | Fails when any committed file differs from what `generate.ts` produces. |

## How they were generated

All files are written by one script, with Bun and `node:crypto` only:

```sh
bun test-vectors/ck-catalog-v1/generate.ts          # rewrite the files
bun test-vectors/ck-catalog-v1/generate.ts --check  # exit 1 on drift
```

- **Hashes** are SHA-256 from `node:crypto`, lowercase hex, over UTF-8 bytes.
- **Canonical JSON** comes from the JCS serializer in `generate.ts`, written to
  RFC 8785: property names sorted by UTF-16 code units, strings escaped as
  ECMAScript `JSON.stringify` does (lone surrogates refused), numbers in
  ECMAScript form. Signed documents hold only safe integers, so a conforming
  implementation never has to format a non-integer.
- **Signatures** are Ed25519 (RFC 8032, deterministic) from `node:crypto`, over
  `domain tag || 0x00 || JCS bytes`. The domain tag is the document's schema
  string: `ck-record/v1`, `ck-status/v1`, `ck-registry-key/v1`,
  `ck-key-rotation/v1`, `ck-key-cert/v1` or `ck-status-request/v1`. The
  signing input is printed in hex next to each signed document.
- **Archives** come from the small ustar writer in `generate.ts`: regular files
  only, mode 0644, uid and gid 0, mtime 0, `ck.json` first. `archive.json` →
  `writer` spells out every header byte. Two archive cases (exactly 1,000 and
  1,001 members) are too large to print, so they carry a `build` recipe (the
  valid archive plus empty members `pad/0001`, `pad/0002`, ...) with the SHA-256
  and length of the result; rebuild them with the same writer and compare the
  digest. Archive cases test the reader only: their padding members are not
  listed in `ck.json`, which the later `files_mismatch` check would catch.
- **Expected outcomes** (refusal codes, picks, apply results) are written by
  hand in the case tables of `generate.ts`. The generator then recomputes each
  one with its reference verifier, reader, applier or resolver and stops if
  the two disagree, so a file cannot record a mistake of the reference code
  without someone also writing that mistake down by hand.

The check runs in the repository's TypeScript test gate: `bun test` from the
repository root picks up `generate.test.ts`. To run it alone:

```sh
bun test test-vectors/ck-catalog-v1/generate.test.ts
```

## Checksums

The table below is rewritten by `generate.ts`; the check fails if it is stale.

<!-- sha256:begin (generated by generate.ts) -->
| File | sha256 |
| --- | --- |
| names.json | `4b69ce59ed5de553d490ff61fe7b1b3c7868dc665e4700eb6c43b77813ecccdc` |
| record.json | `279ba8405e45141ccf2edaaee2c615b0405061dc0e564b7d11f1d6d9e82b2c2f` |
| archive.json | `e5ac9b06742cc3dd6fae5aac1d707c51d603350348d02a66beecb024305a1191` |
| status.json | `6e07ced5fe7178ccd76cca415644f8bf36ef44e532f0155a70345de939d6644b` |
| statements.json | `89780229b796d1cb4ac25c3c8fe742c3dc19ce0fe6dbe51f336c134b51989d0f` |
| resolver.json | `be1e2296110dcca2a6c9fbc1df72200485a5ae1779a3b36bf8411f78ef215413` |
| starter-bundle.json | `d8ad7a8e1036d642a56ad2fd84db08d6a229b4106fbb5bf27e4675578af39108` |
<!-- sha256:end -->

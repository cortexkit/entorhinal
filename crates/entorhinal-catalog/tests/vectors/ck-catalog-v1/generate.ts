#!/usr/bin/env bun
// Reference generator for the CK catalog contract v1 test vectors.
//
//   bun test-vectors/ck-catalog-v1/generate.ts           rewrite the vector files
//   bun test-vectors/ck-catalog-v1/generate.ts --check   exit 1 if a committed file differs
//
// Every value in the vector files is computed here: hashes with SHA-256 and
// signatures with Ed25519 from node:crypto, canonical JSON with the RFC 8785
// (JCS) serializer below. Expected outcomes are written by hand in the case
// tables and then re-derived by the small reference implementations in this
// file; generation stops if the two disagree, so a vector cannot silently
// record what a buggy reference implementation happened to do.
//
// The CK registry (TypeScript) and Entorhinal (Rust) must both reproduce these
// bytes. The signing seeds below are published TEST-ONLY keys: anything signed
// by them proves nothing and must never be trusted outside these vectors.

import { createHash, createPrivateKey, createPublicKey, type KeyObject, sign, verify } from "node:crypto"
import { readFileSync, writeFileSync } from "node:fs"
import { dirname, join } from "node:path"
import { fileURLToPath } from "node:url"

export const VECTOR_DIR = dirname(fileURLToPath(import.meta.url))

// ───────────────────────────── hashing and hex ─────────────────────────────

export function sha256Hex(data: string | Uint8Array): string {
  return createHash("sha256").update(typeof data === "string" ? Buffer.from(data, "utf8") : data).digest("hex")
}

const LOWER_HEX = /^[0-9a-f]*$/

function isLowerHex(value: unknown, length: number): value is string {
  return typeof value === "string" && value.length === length && LOWER_HEX.test(value)
}

// ───────────────────────────── JCS (RFC 8785) ─────────────────────────────

// Strings are serialized as ECMAScript JSON.stringify does, which is exactly
// what RFC 8785 section 3.2.2.2 prescribes, except that a lone surrogate is an
// error in JCS while JSON.stringify would escape it.
function jcsString(value: string): string {
  for (let i = 0; i < value.length; i++) {
    const unit = value.charCodeAt(i)
    if (unit >= 0xd800 && unit <= 0xdbff) {
      const next = value.charCodeAt(i + 1)
      if (!(next >= 0xdc00 && next <= 0xdfff)) throw new Error("JCS: lone high surrogate")
      i++
    } else if (unit >= 0xdc00 && unit <= 0xdfff) {
      throw new Error("JCS: lone low surrogate")
    }
  }
  return JSON.stringify(value)
}

// Property names sort by their UTF-16 code units (RFC 8785 section 3.2.3),
// which is plain code-unit comparison of JavaScript strings.
function compareUtf16(a: string, b: string): number {
  return a < b ? -1 : a > b ? 1 : 0
}

export function jcs(value: unknown): string {
  if (value === null) return "null"
  if (typeof value === "boolean") return value ? "true" : "false"
  if (typeof value === "number") {
    if (!Number.isFinite(value)) throw new Error("JCS: non-finite number")
    // ECMAScript Number::toString is the number form RFC 8785 requires; -0 prints as 0.
    return String(value)
  }
  if (typeof value === "string") return jcsString(value)
  if (Array.isArray(value)) return `[${value.map(jcs).join(",")}]`
  if (typeof value === "object") {
    const entries = Object.entries(value as Record<string, unknown>)
    for (const [key, item] of entries) if (item === undefined) throw new Error(`JCS: undefined value at ${key}`)
    entries.sort(([a], [b]) => compareUtf16(a, b))
    return `{${entries.map(([key, item]) => `${jcsString(key)}:${jcs(item)}`).join(",")}}`
  }
  throw new Error(`JCS: unsupported value of type ${typeof value}`)
}

// ───────────── strict JSON parser (duplicate keys and number spellings) ─────────────

// JSON.parse keeps the last of two equal keys. A signed record must be refused
// instead, because two parsers that keep different duplicates would verify
// one set of bytes and act on another.
export function parseStrictJson(text: string): unknown {
  let at = 0
  const fail = (why: string): never => {
    throw new Error(`strict JSON: ${why} at offset ${at}`)
  }
  const ws = () => {
    while (at < text.length && " \t\n\r".includes(text[at])) at++
  }
  const value = (): unknown => {
    ws()
    const c = text[at]
    if (c === "{") {
      at++
      const out: Record<string, unknown> = {}
      ws()
      if (text[at] === "}") {
        at++
        return out
      }
      for (;;) {
        ws()
        if (text[at] !== '"') fail("expected key")
        const key = str()
        if (Object.hasOwn(out, key)) fail(`duplicate key ${JSON.stringify(key)}`)
        ws()
        if (text[at] !== ":") fail("expected colon")
        at++
        out[key] = value()
        ws()
        if (text[at] === ",") {
          at++
          continue
        }
        if (text[at] === "}") {
          at++
          return out
        }
        fail("expected , or }")
      }
    }
    if (c === "[") {
      at++
      const out: unknown[] = []
      ws()
      if (text[at] === "]") {
        at++
        return out
      }
      for (;;) {
        out.push(value())
        ws()
        if (text[at] === ",") {
          at++
          continue
        }
        if (text[at] === "]") {
          at++
          return out
        }
        fail("expected , or ]")
      }
    }
    if (c === '"') return str()
    const literal = /^(?:true|false|null|-?(?:0|[1-9]\d*)(?:\.\d+)?(?:[eE][+-]?\d+)?)/.exec(text.slice(at))
    if (!literal) fail("unexpected token")
    const token = literal![0]
    // Signed documents require integer tokens without fractions, exponents or negative zero.
    // Check before JSON.parse discards the original number spelling.
    if (token === "-0" || /^-?\d.*[.eE]/.test(token)) throw new Error(`strict JSON: non-canonical number token ${token} at offset ${at}`)
    at += token.length
    return JSON.parse(token)
  }
  const str = (): string => {
    const match = /^"(?:[^"\\\u0000-\u001f]|\\(?:["\\/bfnrt]|u[0-9a-fA-F]{4}))*"/.exec(text.slice(at))
    if (!match) fail("bad string")
    at += match![0].length
    return JSON.parse(match![0]) as string
  }
  const out = value()
  ws()
  if (at !== text.length) fail("trailing data")
  return out
}

// ───────────────────────────── Ed25519 keys ─────────────────────────────

// DER wrappers for a raw 32-byte Ed25519 seed (PKCS#8) and public key (SPKI).
const PKCS8_ED25519_PREFIX = Buffer.from("302e020100300506032b657004220420", "hex")
const SPKI_ED25519_PREFIX = Buffer.from("302a300506032b6570032100", "hex")

export interface TestKey {
  label: string
  seedText: string
  seed: Buffer
  privateKey: KeyObject
  publicKeyHex: string
  fingerprint: string
}

// The seed is SHA-256 of a fixed ASCII sentence, so anyone can re-derive it
// from the README without trusting the hex printed in the vector files.
function testKey(label: string, seedText: string): TestKey {
  const seed = createHash("sha256").update(seedText, "utf8").digest()
  const privateKey = createPrivateKey({ key: Buffer.concat([PKCS8_ED25519_PREFIX, seed]), format: "der", type: "pkcs8" })
  const raw = createPublicKey(privateKey).export({ format: "der", type: "spki" }).subarray(SPKI_ED25519_PREFIX.length)
  return { label, seedText, seed, privateKey, publicKeyHex: raw.toString("hex"), fingerprint: keyFingerprint(raw.toString("hex")) }
}

export function keyFingerprint(publicKeyHex: string): string {
  return sha256Hex(Buffer.from(publicKeyHex, "hex"))
}

function publicKeyObject(publicKeyHex: string): KeyObject {
  return createPublicKey({ key: Buffer.concat([SPKI_ED25519_PREFIX, Buffer.from(publicKeyHex, "hex")]), format: "der", type: "spki" })
}

// Every role has its own TEST-ONLY keys, so no vector passes because two
// roles happen to share a key. Publisher keys 1 and 2 keep the seed sentences
// of the first vector release, so their public keys did not change.
export const TEST_KEY_1 = testKey("TEST-ONLY publisher key 1", "ck-catalog-v1 TEST-ONLY key 1. Published on purpose. Never trust anything it signs.")
export const TEST_KEY_2 = testKey("TEST-ONLY publisher key 2", "ck-catalog-v1 TEST-ONLY key 2. Published on purpose. Never trust anything it signs.")
export const TEST_KEY_3 = testKey("TEST-ONLY publisher key 3", "ck-catalog-v1 TEST-ONLY publisher key 3. Published on purpose. Never trust anything it signs.")
export const TEST_ROOT_KEY = testKey("TEST-ONLY @cortexkit root key", "ck-catalog-v1 TEST-ONLY @cortexkit ROOT key. Published on purpose. Never trust anything it signs.")
export const TEST_CORTEXKIT_KEY_1 = testKey("TEST-ONLY @cortexkit publish key 1", "ck-catalog-v1 TEST-ONLY @cortexkit publish key 1. Published on purpose. Never trust anything it signs.")
export const TEST_CORTEXKIT_KEY_2 = testKey("TEST-ONLY @cortexkit publish key 2", "ck-catalog-v1 TEST-ONLY @cortexkit publish key 2. Published on purpose. Never trust anything it signs.")
export const TEST_REGISTRY_KEY_1 = testKey("TEST-ONLY registry status key 1", "ck-catalog-v1 TEST-ONLY registry status key 1. Published on purpose. Never trust anything it signs.")
export const TEST_REGISTRY_KEY_2 = testKey("TEST-ONLY registry status key 2", "ck-catalog-v1 TEST-ONLY registry status key 2. Published on purpose. Never trust anything it signs.")
export const TEST_REGISTRY_KEY_3 = testKey("TEST-ONLY registry status key 3", "ck-catalog-v1 TEST-ONLY registry status key 3. Published on purpose. Never trust anything it signs.")

// Signed objects are domain separated: the signature covers the ASCII domain
// tag, one 0x00 byte, then the JCS bytes. JCS output never contains a raw 0x00
// byte, so the split is unambiguous, and a signature made for one kind of
// document never verifies as another.
export const DOMAIN_RECORD = "ck-record/v1"
export const DOMAIN_STATUS = "ck-status/v1"
export const DOMAIN_REGISTRY_KEY = "ck-registry-key/v1"
export const DOMAIN_KEY_ROTATION = "ck-key-rotation/v1"
export const DOMAIN_KEY_CERT = "ck-key-cert/v1"
export const DOMAIN_STATUS_REQUEST = "ck-status-request/v1"
export const DOMAIN_STARTER_BUNDLE = "ck-starter-bundle/v1"

export function signingInput(domain: string, canonical: string): Buffer {
  return Buffer.concat([Buffer.from(domain, "ascii"), Buffer.from([0]), Buffer.from(canonical, "utf8")])
}

function signHex(key: TestKey, message: Buffer): string {
  return sign(null, message, key.privateKey).toString("hex")
}

function verifyHex(publicKeyHex: string, message: Buffer, signatureHex: string): boolean {
  if (!isLowerHex(signatureHex, 128) || !isLowerHex(publicKeyHex, 64)) return false
  try {
    return verify(null, message, publicKeyObject(publicKeyHex), Buffer.from(signatureHex, "hex"))
  } catch {
    return false
  }
}

// ───────────────────────────── names ─────────────────────────────

// Scope and package-name parts: lowercase ASCII letters and digits in runs
// joined by single hyphens, so "--" can only ever be the separator below.
const NAME_PART = /^[a-z0-9]+(-[a-z0-9]+)*$/
// A scope is at most 39 bytes (GitHub's organization-name limit, so a scope
// claimed by GitHub org proof always fits); a package name at most 64 bytes.
export const SCOPE_MAX = 39
export const PACKAGE_NAME_MAX = 64
// Scopes no claim can obtain. CortexKit holds @cortexkit under its compiled
// root; the others are kept so no publisher can pose as the registry itself.
export const RESERVED_SCOPES = ["cortexkit", "ck", "catalog", "registry"]
export const BASAL_ID_MAX = 46
const HASHED_PREFIX_BYTES = 29
const HASHED_HEX_CHARS = 16

export type NameResult =
  | { ok: true; canonical: string; scope: string; name: string; short: string; shortBytes: number; sha256: string; form: "short" | "hashed"; id: string }
  | { ok: false; code: "name_invalid"; part: "whole" | "scope" | "name" }

// The pure mapping from a package name to its Basal id. ASCII upper case is
// folded first; nothing else is normalized, so any other character outside the
// grammar is refused rather than guessed at.
export function mapName(input: string): NameResult {
  const lowered = input.replace(/[A-Z]/g, (c) => c.toLowerCase())
  const match = /^@([^/]*)\/([^/]*)$/.exec(lowered)
  if (!match) return { ok: false, code: "name_invalid", part: "whole" }
  const [, scope, name] = match
  if (!NAME_PART.test(scope) || scope.length > SCOPE_MAX) return { ok: false, code: "name_invalid", part: "scope" }
  if (!NAME_PART.test(name) || name.length > PACKAGE_NAME_MAX) return { ok: false, code: "name_invalid", part: "name" }
  const canonical = `@${scope}/${name}`
  const short = `${scope}--${name}`
  const digest = sha256Hex(canonical)
  const hashed = short.length > BASAL_ID_MAX
  const id = hashed ? `${short.slice(0, HASHED_PREFIX_BYTES)}_${digest.slice(0, HASHED_HEX_CHARS)}` : short
  return { ok: true, canonical, scope, name, short, shortBytes: short.length, sha256: digest, form: hashed ? "hashed" : "short", id }
}

export type BindResult =
  | { ok: true; id: string; new_binding: boolean }
  | { ok: false; code: "name_invalid" | "name_not_canonical" | "id_bound"; id?: string; bound_to?: string }

// The registry's publish-time binding check: a name must already be canonical,
// and its id must be free or bound to that same name. The stored table is
// authoritative; the check never assumes the mapping is collision free.
export function bindName(table: Record<string, string>, input: string): BindResult {
  const mapped = mapName(input)
  if (!mapped.ok) return { ok: false, code: "name_invalid" }
  if (mapped.canonical !== input) return { ok: false, code: "name_not_canonical" }
  const bound = table[mapped.id]
  if (bound !== undefined && bound !== mapped.canonical) return { ok: false, code: "id_bound", id: mapped.id, bound_to: bound }
  return { ok: true, id: mapped.id, new_binding: bound === undefined }
}

export type ClaimResult = { ok: true } | { ok: false; code: "name_invalid" | "scope_reserved" | "name_not_canonical" }

// The registry's scope-claim check (invitation or GitHub org proof). The
// reserved list is compared after ASCII folding, so a case variant of a
// reserved scope is reported as reserved, not merely as non-canonical.
export function claimScope(input: string): ClaimResult {
  const lowered = input.replace(/[A-Z]/g, (c) => c.toLowerCase())
  if (!NAME_PART.test(lowered) || lowered.length > SCOPE_MAX) return { ok: false, code: "name_invalid" }
  if (RESERVED_SCOPES.includes(lowered)) return { ok: false, code: "scope_reserved" }
  if (lowered !== input) return { ok: false, code: "name_not_canonical" }
  return { ok: true }
}

function namesVectors() {
  const long35 = "abcdefghij-klmnopqrst-uvwxyz-012345" // 35 bytes: "cortexkit--" + 35 = 46
  const scope39 = "abcdefghij-klmnopqrst-uvwxyz-0123456789"
  const scope40 = `${scope39}0`
  const name64 = "package-name-at-the-sixty-four-byte-limit-abcdefghijklmnopqrstuv"
  const name65 = `${name64}w`
  if (scope39.length !== 39 || scope40.length !== 40 || name64.length !== 64 || name65.length !== 65) throw new Error("names: limit fixtures")
  const valid: [string, string][] = [
    ["@cortexkit/swe-head", "ordinary first-party persona name"],
    ["@cortexkit/memory-swe", "the memory profile id used for memory rows"],
    ["@a/b", "shortest possible name"],
    ["@0/9", "digits only"],
    ["@xyz/a-b-c", "single hyphens inside a part"],
    ["@CortexKit/Swe-Head", "ASCII upper case folds to the same id; the registry still refuses it as a published name"],
    [`@cortexkit/${long35}`, "short form exactly 46 bytes: stays short"],
    [`@cortexkit/${long35}6`, "short form 47 bytes: hashed"],
    ["@cortexkit/a-very-long-package-name-that-needs-hashing", "hashed form"],
    ["@cortexkit/a-very-long-package-name-that-needs-hashing-too", "same first 29 bytes as the previous name, different id"],
    ["@cortexkit/abcdefghijklmnopq-rstuvwxyz-0123456789", "hashed form whose 29-byte prefix ends on a hyphen inside the name"],
    ["@abcdefghijklmnopqrstuvwxyz0/a-very-long-tail-name", "29-byte prefix ends on the separator hyphen"],
    [`@${scope39}/a`, "scope exactly 39 bytes, the scope limit"],
    [`@a/${name64}`, "package name exactly 64 bytes, the name limit"],
    [`@${scope39}/${name64}`, "both parts at their limits: the longest valid name (105 bytes), hashed"],
  ]
  const refused: [string, "whole" | "scope" | "name", string][] = [
    ["", "whole", "empty"],
    ["cortexkit/swe-head", "whole", "missing @"],
    ["@cortexkit", "whole", "missing /name"],
    ["@cortexkit/swe/head", "whole", "two slashes"],
    ["@@cortexkit/swe-head", "scope", "doubled @"],
    [" @cortexkit/swe-head", "whole", "leading space; nothing is trimmed"],
    ["@/swe-head", "scope", "empty scope"],
    ["@cortexkit/", "name", "empty name"],
    ["@-a/b", "scope", "leading hyphen in scope"],
    ["@a-/b", "scope", "trailing hyphen in scope (R1 example)"],
    ["@a/-b", "name", "leading hyphen in name (R1 example)"],
    ["@a/b-", "name", "trailing hyphen in name"],
    ["@a--b/c", "scope", "doubled hyphen in scope"],
    ["@a/b--c", "name", "doubled hyphen in name"],
    ["@a/b_c", "name", "underscore: only the hashed id form may contain _"],
    ["@a/b.c", "name", "dot"],
    ["@a/b c", "name", "space"],
    ["@ä/b", "scope", "non-ASCII letter"],
    ["@Ä/b", "scope", "non-ASCII upper case is not folded"],
    [`@${scope40}/a`, "scope", "scope 40 bytes, one over the limit"],
    [`@a/${name65}`, "name", "package name 65 bytes, one over the limit"],
    [`@${scope40}/${name65}`, "scope", "both parts too long: the scope is checked first"],
  ]
  const validOut = valid.map(([input, note]) => {
    const r = mapName(input)
    if (!r.ok) throw new Error(`names: expected ${input} to map`)
    return {
      input,
      note,
      canonical: r.canonical,
      input_is_canonical: r.canonical === input,
      short: r.short,
      short_bytes: r.shortBytes,
      sha256_of_canonical: r.sha256,
      form: r.form,
      basal_id: r.id,
      basal_id_bytes: r.id.length,
    }
  })
  // Hand-written expectations that the computed rows must agree with.
  const expectForm: Record<string, "short" | "hashed"> = {
    "@cortexkit/swe-head": "short",
    [`@cortexkit/${long35}`]: "short",
    [`@cortexkit/${long35}6`]: "hashed",
  }
  for (const row of validOut) {
    if (expectForm[row.input] && expectForm[row.input] !== row.form) throw new Error(`names: ${row.input} form`)
    if (row.basal_id_bytes > BASAL_ID_MAX) throw new Error(`names: ${row.input} id too long`)
    if (row.form === "short" && row.basal_id.includes("_")) throw new Error("names: short id with _")
  }
  if (validOut.find((r) => r.input === "@cortexkit/memory-swe")?.basal_id !== "cortexkit--memory-swe") throw new Error("names: R19 example")
  if (!validOut.find((r) => r.input === "@cortexkit/abcdefghijklmnopq-rstuvwxyz-0123456789")?.basal_id.slice(27, 30).endsWith("q-_")) {
    throw new Error("names: prefix ending on a hyphen")
  }
  if (validOut.find((r) => r.input === "@abcdefghijklmnopqrstuvwxyz0/a-very-long-tail-name")?.basal_id.slice(27, 30) !== "--_") {
    throw new Error("names: prefix ending on the separator")
  }
  const shared = validOut.filter((r) => r.input.startsWith("@cortexkit/a-very-long-package-name"))
  if (shared[0].basal_id.slice(0, 29) !== shared[1].basal_id.slice(0, 29) || shared[0].basal_id === shared[1].basal_id) {
    throw new Error("names: shared-prefix pair")
  }
  const refusedOut = refused.map(([input, part, note]) => {
    const r = mapName(input)
    if (r.ok || r.part !== part) throw new Error(`names: expected ${JSON.stringify(input)} refused at ${part}, got ${JSON.stringify(r)}`)
    return { input, note, code: r.code, part }
  })

  const hashedA = mapName("@cortexkit/a-very-long-package-name-that-needs-hashing")
  if (!hashedA.ok) throw new Error("unreachable")
  const bindingCases: { name: string; note: string; table: Record<string, string>; publish: string; expect: BindResult }[] = [
    { name: "first_publish_binds", note: "id is free: the publish binds it", table: {}, publish: "@acme/tools", expect: { ok: true, id: "acme--tools", new_binding: true } },
    {
      name: "same_name_again",
      note: "a later version of the same package reuses its binding",
      table: { "acme--tools": "@acme/tools" },
      publish: "@acme/tools",
      expect: { ok: true, id: "acme--tools", new_binding: false },
    },
    {
      name: "short_id_bound_to_other_name",
      note: "PLANTED ROW: no two valid names share a short id, so this table can only arise from a corrupt or migrated row; the check must read the stored table, not recompute",
      table: { "acme--tools": "@acme-legacy/tools" },
      publish: "@acme/tools",
      expect: { ok: false, code: "id_bound", id: "acme--tools", bound_to: "@acme-legacy/tools" },
    },
    {
      name: "hashed_id_bound_to_other_name",
      note: "PLANTED ROW standing in for a 64-bit truncated-hash collision, which cannot be computed: the binding refuses the second name",
      table: { [hashedA.id]: "@cortexkit/a-very-long-package-name-that-needs-hashing-x" },
      publish: "@cortexkit/a-very-long-package-name-that-needs-hashing",
      expect: { ok: false, code: "id_bound", id: hashedA.id, bound_to: "@cortexkit/a-very-long-package-name-that-needs-hashing-x" },
    },
    {
      name: "upper_case_publish_refused",
      note: "the mapping folds case, but a published name must already be canonical",
      table: { "acme--tools": "@acme/tools" },
      publish: "@Acme/Tools",
      expect: { ok: false, code: "name_not_canonical" },
    },
    { name: "invalid_name_refused", note: "grammar refusal comes before the binding check", table: {}, publish: "@acme/to_ols", expect: { ok: false, code: "name_invalid" } },
  ]
  for (const c of bindingCases) {
    const got = bindName(c.table, c.publish)
    if (JSON.stringify(got) !== JSON.stringify(c.expect)) throw new Error(`names binding ${c.name}: got ${JSON.stringify(got)}`)
  }

  const claimCases: { scope: string; note: string; expect: ClaimResult }[] = [
    { scope: "acme", note: "ordinary claim", expect: { ok: true } },
    { scope: "cortexkit", note: "reserved: held by CortexKit under its compiled root", expect: { ok: false, code: "scope_reserved" } },
    { scope: "ck", note: "reserved", expect: { ok: false, code: "scope_reserved" } },
    { scope: "catalog", note: "reserved", expect: { ok: false, code: "scope_reserved" } },
    { scope: "registry", note: "reserved", expect: { ok: false, code: "scope_reserved" } },
    { scope: "CortexKit", note: "a case variant of a reserved scope is reported as reserved", expect: { ok: false, code: "scope_reserved" } },
    { scope: "ck-tools", note: "only exact reserved names are reserved", expect: { ok: true } },
    { scope: "catalogs", note: "only exact reserved names are reserved", expect: { ok: true } },
    { scope: scope39, note: "39 bytes: at the limit", expect: { ok: true } },
    { scope: scope40, note: "40 bytes: over the limit", expect: { ok: false, code: "name_invalid" } },
    { scope: "ac_me", note: "outside the grammar", expect: { ok: false, code: "name_invalid" } },
    { scope: "Acme", note: "a claimed scope must already be canonical", expect: { ok: false, code: "name_not_canonical" } },
  ]
  for (const c of claimCases) {
    const got = claimScope(c.scope)
    if (JSON.stringify(got) !== JSON.stringify(c.expect)) throw new Error(`names claim ${c.scope}: got ${JSON.stringify(got)}`)
  }

  return {
    schema: "ck-catalog-v1/names",
    rules: {
      part_grammar: "^[a-z0-9]+(-[a-z0-9]+)*$",
      scope_max_bytes: SCOPE_MAX,
      package_name_max_bytes: PACKAGE_NAME_MAX,
      check_order: "split on the single '/', then the scope (grammar and length), then the name (grammar and length); name_invalid names the first part that fails",
      reserved_scopes: RESERVED_SCOPES,
      scope_claim_order: "fold ASCII case; grammar and length (name_invalid); reserved list on the folded scope (scope_reserved); the claimed text must equal the folded text (name_not_canonical)",
      input_folding: "ASCII A-Z to a-z only, before validation; nothing else is normalized or trimmed",
      short_form: "<scope>--<name>",
      basal_id_max_bytes: BASAL_ID_MAX,
      hashed_form: "first 29 bytes of the short form, '_', first 16 lowercase hex chars of SHA-256(UTF-8 of canonical '@scope/name')",
    },
    valid: validOut,
    refused: refusedOut,
    binding: bindingCases,
    scope_claims: claimCases,
  }
}

// ───────────────────────────── archives ─────────────────────────────

// Paths are shared by archive members, `files` entries in ck.json and the
// packer: ASCII components of [A-Za-z0-9._-] with no empty, "." or ".."
// component, at most 255 bytes, and storable in a plain ustar header (a name
// of at most 100 bytes, optionally after a prefix of at most 155 bytes that
// ends at a "/"). Plain ustar is the only form allowed because pax and GNU
// long-name headers are refused.
export const PATH_MAX = 255
export const MAX_MEMBERS = 1000
const PATH_COMPONENT = /^[A-Za-z0-9._-]+$/

export type PathRule = "path_invalid" | "path_too_long" | "path_not_ustar"

// The ustar split: the path itself when it fits the 100-byte name field,
// otherwise the first "/" that leaves at most 100 bytes after it and at most
// 155 before it.
export function ustarSplit(path: string): { prefix: string; name: string } | null {
  if (path.length <= 100) return { prefix: "", name: path }
  for (let i = Math.max(1, path.length - 101); i <= Math.min(155, path.length - 2); i++) {
    if (path[i] === "/") return { prefix: path.slice(0, i), name: path.slice(i + 1) }
  }
  return null
}

export function checkPath(path: string): { ok: true } | { ok: false; rule: PathRule } {
  if (path === "" || path.split("/").some((part) => !PATH_COMPONENT.test(part) || part === "." || part === "..")) return { ok: false, rule: "path_invalid" }
  // Every allowed character is one ASCII byte, so length is the byte count.
  if (path.length > PATH_MAX) return { ok: false, rule: "path_too_long" }
  if (!ustarSplit(path)) return { ok: false, rule: "path_not_ustar" }
  return { ok: true }
}

// One ustar header. The vectors' writer fixes every field (mode 0644, uid and
// gid 0, mtime 0, empty owner names), so archive bytes depend only on member
// paths and contents. The optional overrides exist only to build the
// malformed archives of archive.json.
export interface HeaderSpec {
  path?: string
  prefix?: string
  name?: string
  size: number
  typeflag?: string
  linkname?: string
  magic?: string
  version?: string
  sizeField?: Buffer
  checksumDelta?: number
}

function tarHeader(spec: HeaderSpec): Buffer {
  let prefix = spec.prefix ?? ""
  let name = spec.name ?? ""
  if (spec.path !== undefined) {
    const split = ustarSplit(spec.path)
    if (!split) throw new Error(`tar: path does not fit a ustar header: ${spec.path}`)
    ;({ prefix, name } = split)
  }
  if (Buffer.byteLength(name, "latin1") > 100 || Buffer.byteLength(prefix, "latin1") > 155) throw new Error("tar: name or prefix field overflow")
  const h = Buffer.alloc(512)
  h.write(name, 0, "latin1")
  h.write("0000644\0", 100, "ascii")
  h.write("0000000\0", 108, "ascii")
  h.write("0000000\0", 116, "ascii")
  if (spec.sizeField) spec.sizeField.copy(h, 124)
  else h.write(`${spec.size.toString(8).padStart(11, "0")}\0`, 124, "ascii")
  h.write("00000000000\0", 136, "ascii")
  h.write("        ", 148, "ascii")
  h.write(spec.typeflag ?? "0", 156, "latin1")
  if (spec.linkname) h.write(spec.linkname, 157, "latin1")
  h.write(spec.magic ?? "ustar\0", 257, "latin1")
  h.write(spec.version ?? "00", 263, "latin1")
  h.write(prefix, 345, "latin1")
  let sum = 0
  for (const byte of h) sum += byte
  h.write(`${(sum + (spec.checksumDelta ?? 0)).toString(8).padStart(6, "0")}\0 `, 148, "ascii")
  return h
}

export interface TarEntry {
  header: HeaderSpec
  data: Buffer
}

export function tarEntry(path: string, content: string | Buffer, extra: Partial<HeaderSpec> = {}): TarEntry {
  const data = typeof content === "string" ? Buffer.from(content, "utf8") : content
  return { header: { path, size: data.length, ...extra }, data }
}

// Entries, each padded to a 512-byte boundary, then the tail (by default the
// two zero blocks that end an archive).
export function writeTar(entries: TarEntry[], tail: Buffer = Buffer.alloc(1024)): Buffer {
  const parts: Buffer[] = []
  for (const e of entries) parts.push(tarHeader(e.header), e.data, Buffer.alloc((512 - (e.data.length % 512)) % 512))
  parts.push(tail)
  return Buffer.concat(parts)
}

export function buildTar(members: { path: string; content: string }[]): Buffer {
  return writeTar(members.map((m) => tarEntry(m.path, m.content)))
}

export type ArchiveRule =
  | "header_checksum"
  | "magic"
  | "member_type"
  | "size_encoding"
  | PathRule
  | "too_many_members"
  | "path_duplicate"
  | "path_collision"
  | "truncated"
  | "terminator_missing"
  | "trailing_data"
  | "ck_json_missing"

export type ArchiveResult =
  | { ok: true; members: Map<string, Buffer> }
  | { ok: false; code: "archive_invalid"; rule: ArchiveRule; offset?: number; member?: string; typeflag?: string }

// A numeric header field: one or more octal digits from the field's first
// byte, then only NUL or space bytes, at least one of them. Base-256 sizes
// (first byte 0x80 or 0xff), leading spaces and unterminated fields fail.
function octalField(field: Buffer): number | null {
  let i = 0
  while (i < field.length && field[i] >= 0x30 && field[i] <= 0x37) i++
  if (i === 0 || i === field.length) return null
  for (let j = i; j < field.length; j++) if (field[j] !== 0 && field[j] !== 0x20) return null
  const n = Number.parseInt(field.subarray(0, i).toString("latin1"), 8)
  return Number.isSafeInteger(n) ? n : null
}

function textField(header: Buffer, start: number, length: number): string {
  const field = header.subarray(start, start + length)
  const nul = field.indexOf(0)
  return field.subarray(0, nul < 0 ? length : nul).toString("latin1")
}

// The strict reader every party runs before trusting an archive. It walks
// the archive once, header by header, and refuses at the first rule that
// fails, so every implementation reports the same rule at the same offset.
export function readArchive(archive: Buffer, requiredMember: string | null = "ck.json"): ArchiveResult {
  const fail = (rule: ArchiveRule, extra: { offset?: number; member?: string; typeflag?: string } = {}): ArchiveResult => ({ ok: false, code: "archive_invalid", rule, ...extra })
  const members = new Map<string, Buffer>()
  const byLowerCase = new Map<string, string>()
  let at = 0
  for (;;) {
    if (archive.length - at < 512) return fail("terminator_missing", { offset: at })
    const header = archive.subarray(at, at + 512)
    if (header.every((b) => b === 0)) {
      const second = archive.subarray(at + 512, at + 1024)
      if (second.length < 512 || !second.every((b) => b === 0)) return fail("terminator_missing", { offset: at })
      for (let i = at + 1024; i < archive.length; i++) if (archive[i] !== 0) return fail("trailing_data", { offset: i })
      break
    }
    let sum = 0
    for (let i = 0; i < 512; i++) sum += i >= 148 && i < 156 ? 0x20 : header[i]
    if (octalField(header.subarray(148, 156)) !== sum) return fail("header_checksum", { offset: at })
    if (header.subarray(257, 263).toString("latin1") !== "ustar\0" || header.subarray(263, 265).toString("latin1") !== "00") return fail("magic", { offset: at })
    const typeflag = String.fromCharCode(header[156])
    if (typeflag !== "0" && typeflag !== "\0") return fail("member_type", { offset: at, typeflag })
    const size = octalField(header.subarray(124, 136))
    if (size === null) return fail("size_encoding", { offset: at })
    const name = textField(header, 0, 100)
    const prefix = textField(header, 345, 155)
    const path = prefix ? `${prefix}/${name}` : name
    const pathCheck = checkPath(path)
    if (!pathCheck.ok) return fail(pathCheck.rule, { offset: at, member: path })
    if (members.size + 1 > MAX_MEMBERS) return fail("too_many_members", { offset: at, member: path })
    const seen = byLowerCase.get(path.toLowerCase())
    if (seen !== undefined) return fail(seen === path ? "path_duplicate" : "path_collision", { offset: at, member: path })
    byLowerCase.set(path.toLowerCase(), path)
    if (at + 512 + size > archive.length) return fail("truncated", { offset: at, member: path })
    members.set(path, archive.subarray(at + 512, at + 512 + size))
    at += 512 + Math.ceil(size / 512) * 512
  }
  if (requiredMember !== null && !members.has(requiredMember)) return fail("ck_json_missing")
  return { ok: true, members }
}

// ───────────────────────────── packages used by the vectors ─────────────────────────────

interface VectorPackage {
  label: string
  manifestText: string
  manifest: Record<string, unknown>
  members: { path: string; content: string }[]
  archive: Buffer
  flowJson?: { id: string; version: number }
}

function makePackage(label: string, envelope: Record<string, unknown>, contentFiles: { path: string; content: string }[]): VectorPackage {
  const files = contentFiles.map((f) => ({ path: f.path, sha256: sha256Hex(f.content), bytes: Buffer.byteLength(f.content) }))
  const { body, ...head } = envelope
  const manifest = { ...head, files, body }
  // ck.json is authored text, hashed exactly as stored; it is not canonicalized.
  const manifestText = `${JSON.stringify(manifest, null, 2)}\n`
  const members = [{ path: "ck.json", content: manifestText }, ...contentFiles]
  const flow = contentFiles.find((f) => f.path === "flow.json")
  return {
    label,
    manifestText,
    manifest,
    members,
    archive: buildTar(members),
    flowJson: flow ? (JSON.parse(flow.content) as { id: string; version: number }) : undefined,
  }
}

const SKILL_MD = "# Hello skill\n\nTest-vector skill. Symptom: none. Checks: run `scripts/hello.sh`. Fix: none.\n"
const HELLO_SH = "#!/bin/bash\necho hello\n"

export const SKILL_PACKAGE = makePackage(
  "skill",
  {
    schema: "ck-package/v1",
    name: "@example/hello-skill",
    version: "1.0.0",
    kind: "skill",
    summary: "Test-vector skill that says hello",
    license: "MIT",
    source: "https://example.invalid/hello-skill",
    requires: { capabilities: ["cap/example.read@1"], packages: {} },
    body: { entry: "SKILL.md", audience: "user", scripts: [{ path: "scripts/hello.sh", runs_with: "bash" }] },
  },
  [
    { path: "SKILL.md", content: SKILL_MD },
    { path: "scripts/hello.sh", content: HELLO_SH },
  ],
)

const FLOW_ID = (() => {
  const r = mapName("@example/hello-flow")
  if (!r.ok) throw new Error("unreachable")
  return r.id
})()

export const FLOW_PACKAGE = makePackage(
  "flow",
  {
    schema: "ck-package/v1",
    name: "@example/hello-flow",
    version: "1.0.0",
    kind: "flow",
    summary: "Test-vector flow; only its id and version are exercised",
    license: "MIT",
    source: "https://example.invalid/hello-flow",
    requires: { capabilities: ["cap/example.read@1"], packages: {} },
    body: { manifest: "flow.json", script: "flow.js" },
  },
  [
    // The publisher's tool authored the mapped id and the integer version (3,
    // not a publish counter) before signing; nothing rewrites them later.
    { path: "flow.json", content: `${JSON.stringify({ format: 1, id: FLOW_ID, version: 3, purpose: "Test-vector flow" }, null, 2)}\n` },
    { path: "flow.js", content: "// test-vector flow script\n" },
  ],
)

// ───────────────────────────── ck-record/v1 ─────────────────────────────

const RECORD_FIELDS = ["schema", "name", "version", "manifest_sha256", "archive_sha256", "archive_bytes", "public_key", "log_head"]

export function recordFor(pkg: VectorPackage, key: TestKey): Record<string, unknown> {
  const record: Record<string, unknown> = {
    schema: "ck-record/v1",
    name: pkg.manifest.name,
    version: pkg.manifest.version,
    manifest_sha256: sha256Hex(pkg.manifestText),
    archive_sha256: sha256Hex(pkg.archive),
    archive_bytes: pkg.archive.length,
    public_key: key.publicKeyHex,
    log_head: null,
  }
  if (pkg.flowJson) record.flow_version = pkg.flowJson.version
  return record
}

export type VerifyResult = { ok: true } | { ok: false; code: string; field?: string }

// The reference verifier, in the order the contract fixes: shape, signature,
// key trust, then the cross-checks against the manifest and archive bytes.
export function verifyRecord(recordText: string, signatureHex: string, pkg: VectorPackage, trustedFingerprints: string[]): VerifyResult {
  let parsed: unknown
  try {
    parsed = parseStrictJson(recordText)
  } catch {
    // Duplicate keys and non-canonical number tokens are both strict-parse failures.
    return { ok: false, code: "record_invalid", field: "json" }
  }
  if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed)) return { ok: false, code: "record_invalid", field: "json" }
  const r = parsed as Record<string, unknown>
  for (const key of Object.keys(r)) if (!RECORD_FIELDS.includes(key) && key !== "flow_version") return { ok: false, code: "record_invalid", field: key }
  for (const key of RECORD_FIELDS) if (!Object.hasOwn(r, key)) return { ok: false, code: "record_invalid", field: key }
  if (r.schema !== "ck-record/v1") return { ok: false, code: "record_invalid", field: "schema" }
  const mapped = typeof r.name === "string" ? mapName(r.name) : null
  if (!mapped?.ok || mapped.canonical !== r.name) return { ok: false, code: "record_invalid", field: "name" }
  if (typeof r.version !== "string" || !parseVersion(r.version)) return { ok: false, code: "record_invalid", field: "version" }
  for (const key of ["manifest_sha256", "archive_sha256", "public_key"]) if (!isLowerHex(r[key], 64)) return { ok: false, code: "record_invalid", field: key }
  if (!Number.isSafeInteger(r.archive_bytes) || (r.archive_bytes as number) <= 0) return { ok: false, code: "record_invalid", field: "archive_bytes" }
  if (Object.hasOwn(r, "flow_version") && (!Number.isSafeInteger(r.flow_version) || (r.flow_version as number) <= 0)) {
    return { ok: false, code: "record_invalid", field: "flow_version" }
  }
  if (!verifyHex(r.public_key as string, signingInput(DOMAIN_RECORD, jcs(r)), signatureHex)) return { ok: false, code: "signature_invalid" }
  if (!trustedFingerprints.includes(keyFingerprint(r.public_key as string))) return { ok: false, code: "key_unknown" }
  if (sha256Hex(pkg.manifestText) !== r.manifest_sha256) return { ok: false, code: "record_mismatch", field: "manifest_sha256" }
  if (sha256Hex(pkg.archive) !== r.archive_sha256) return { ok: false, code: "record_mismatch", field: "archive_sha256" }
  if (pkg.archive.length !== r.archive_bytes) return { ok: false, code: "record_mismatch", field: "archive_bytes" }
  const read = readArchive(pkg.archive)
  if (!read.ok) return { ok: false, code: "archive_invalid", field: read.rule }
  const members = read.members
  if (members.get("ck.json")?.toString("utf8") !== pkg.manifestText) return { ok: false, code: "record_mismatch", field: "ck.json" }
  if (pkg.manifest.name !== r.name) return { ok: false, code: "record_mismatch", field: "name" }
  if (pkg.manifest.version !== r.version) return { ok: false, code: "record_mismatch", field: "version" }
  const isFlow = pkg.manifest.kind === "flow"
  if (isFlow !== Object.hasOwn(r, "flow_version")) return { ok: false, code: "record_mismatch", field: "flow_version" }
  if (isFlow) {
    if (pkg.flowJson?.id !== mapped.id) return { ok: false, code: "record_mismatch", field: "flow.json id" }
    if (pkg.flowJson?.version !== r.flow_version) return { ok: false, code: "record_mismatch", field: "flow_version" }
  }
  const listed = new Set<string>()
  for (const entry of pkg.manifest.files as { path: string; sha256: string; bytes: number }[]) {
    const bytes = members.get(entry.path)
    if (!bytes || sha256Hex(bytes) !== entry.sha256 || bytes.length !== entry.bytes) return { ok: false, code: "files_mismatch", field: entry.path }
    listed.add(entry.path)
  }
  for (const path of members.keys()) if (path !== "ck.json" && !listed.has(path)) return { ok: false, code: "files_mismatch", field: path }
  return { ok: true }
}

function signCanonical(value: unknown, key: TestKey, domain: string): { canonical: string; signature: string } {
  const canonical = jcs(value)
  return { canonical, signature: signHex(key, signingInput(domain, canonical)) }
}

function flipFirstBit(signatureHex: string): string {
  return ((Number.parseInt(signatureHex.slice(0, 2), 16) ^ 0x01).toString(16).padStart(2, "0") + signatureHex.slice(2))
}

function packageOut(pkg: VectorPackage) {
  return {
    members: pkg.members.map((m) => ({ path: m.path, content: m.content, sha256: sha256Hex(m.content), bytes: Buffer.byteLength(m.content) })),
    manifest_text: pkg.manifestText,
    manifest_sha256: sha256Hex(pkg.manifestText),
    archive_base64: pkg.archive.toString("base64"),
    archive_sha256: sha256Hex(pkg.archive),
    archive_bytes: pkg.archive.length,
  }
}

function keyOut(key: TestKey) {
  return {
    WARNING: "TEST-ONLY KEY. The seed is published. Never trust anything signed by it outside these vectors.",
    label: key.label,
    seed_derivation: "SHA-256 of the UTF-8 bytes of seed_text",
    seed_text: key.seedText,
    seed_hex: key.seed.toString("hex"),
    public_key_hex: key.publicKeyHex,
    fingerprint: key.fingerprint,
  }
}

function jcsVectors() {
  const cases: { name: string; input_json: string; expect: string }[] = [
    {
      name: "keys_sort_by_utf16_code_units",
      input_json: '{"\\u20ac":"euro","\\r":"cr","\\ufb33":"hebrew","1":"one","\\ud83d\\ude00":"smiley","\\u0080":"c1","\\u00f6":"o-umlaut"}',
      // U+1F600 is the surrogate pair D83D DE00, which sorts before U+FB33 by
      // UTF-16 code units even though its code point is higher.
      expect: '{"\\r":"cr","1":"one","\u0080":"c1","\u00f6":"o-umlaut","\u20ac":"euro","\ud83d\ude00":"smiley","\ufb33":"hebrew"}',
    },
    {
      name: "string_escaping",
      input_json: '{"s":"tab\\t nl\\n quote\\" backslash\\\\ ctl\\u001f del\\u007f slash\\/ e\\u00e9"}',
      expect: '{"s":"tab\\t nl\\n quote\\" backslash\\\\ ctl\\u001f del\u007f slash/ e\u00e9"}',
    },
    {
      name: "nesting_whitespace_and_literals",
      input_json: '{ "b": [ ], "a": { "z": null, "y": true, "x": false }, "c": {} }',
      expect: '{"a":{"x":false,"y":true,"z":null},"b":[],"c":{}}',
    },
    { name: "arrays_keep_order", input_json: '[3, 1, 2, {"b": 1, "a": 2}]', expect: '[3,1,2,{"a":2,"b":1}]' },
    {
      name: "safe_integer_bounds",
      input_json: '{"max":9007199254740991,"min":-9007199254740991,"zero":0}',
      expect: '{"max":9007199254740991,"min":-9007199254740991,"zero":0}',
    },
  ]
  return cases.map((c) => {
    const canonical = jcs(parseStrictJson(c.input_json))
    if (canonical !== c.expect) throw new Error(`jcs ${c.name}: got ${canonical}`)
    return { name: c.name, input_json: c.input_json, canonical, canonical_utf8_hex: Buffer.from(canonical, "utf8").toString("hex"), sha256: sha256Hex(canonical) }
  })
}

function recordVectors() {
  const trusted = [TEST_KEY_1.fingerprint]
  const packages: Record<string, VectorPackage> = { skill: SKILL_PACKAGE, flow: FLOW_PACKAGE }

  const valid = (["skill", "flow"] as const).map((label) => {
    const record = recordFor(packages[label], TEST_KEY_1)
    const { canonical, signature } = signCanonical(record, TEST_KEY_1, DOMAIN_RECORD)
    const result = verifyRecord(canonical, signature, packages[label], trusted)
    if (!result.ok) throw new Error(`record ${label}: ${JSON.stringify(result)}`)
    return {
      name: `${label}_record`,
      package: label,
      record,
      canonical,
      canonical_sha256: sha256Hex(canonical),
      signing_input_hex: signingInput(DOMAIN_RECORD, canonical).toString("hex"),
      signature,
      key_fingerprint: TEST_KEY_1.fingerprint,
    }
  })

  const skillRecord = recordFor(SKILL_PACKAGE, TEST_KEY_1)
  const skillSigned = signCanonical(skillRecord, TEST_KEY_1, DOMAIN_RECORD)
  const flowRecord = recordFor(FLOW_PACKAGE, TEST_KEY_1)
  const signed = (record: Record<string, unknown>, key = TEST_KEY_1) => signCanonical(record, key, DOMAIN_RECORD)
  const without = (record: Record<string, unknown>, field: string) => {
    const copy = { ...record }
    delete copy[field]
    return copy
  }
  const reordered = Object.fromEntries(Object.entries(skillRecord).reverse())

  type Case = { name: string; note: string; package: "skill" | "flow"; served_text: string; signature: string; expect: VerifyResult }
  const cases: Case[] = [
    {
      name: "served_with_other_key_order_and_whitespace",
      note: "verifiers canonicalize the parsed record; the served text's layout does not matter",
      package: "skill",
      served_text: JSON.stringify(reordered, null, 2),
      signature: skillSigned.signature,
      expect: { ok: true },
    },
    { name: "bad_signature_bit_flip", note: "first signature byte XOR 0x01", package: "skill", served_text: skillSigned.canonical, signature: flipFirstBit(skillSigned.signature), expect: { ok: false, code: "signature_invalid" } },
    {
      name: "wrong_domain_status_tag",
      note: "signed over ck-status/v1 0x00 JCS instead of ck-record/v1 0x00 JCS",
      package: "skill",
      served_text: skillSigned.canonical,
      signature: signHex(TEST_KEY_1, signingInput(DOMAIN_STATUS, skillSigned.canonical)),
      expect: { ok: false, code: "signature_invalid" },
    },
    {
      name: "wrong_domain_no_tag",
      note: "signed over the bare JCS bytes with no domain tag",
      package: "skill",
      served_text: skillSigned.canonical,
      signature: signHex(TEST_KEY_1, Buffer.from(skillSigned.canonical, "utf8")),
      expect: { ok: false, code: "signature_invalid" },
    },
    {
      name: "schema_names_another_document",
      note: "validly signed record whose schema field is not ck-record/v1",
      package: "skill",
      ...served(signed({ ...skillRecord, schema: "ck-status/v1" })),
      expect: { ok: false, code: "record_invalid", field: "schema" },
    },
    { name: "unknown_field", note: "extra field, validly signed", package: "skill", ...served(signed({ ...skillRecord, kind: "skill" })), expect: { ok: false, code: "record_invalid", field: "kind" } },
    { name: "log_head_absent", note: "the reserved field must be present (null in v1)", package: "skill", ...served(signed(without(skillRecord, "log_head"))), expect: { ok: false, code: "record_invalid", field: "log_head" } },
    {
      name: "uppercase_hex_digest",
      note: "hex values are lowercase only",
      package: "skill",
      ...served(signed({ ...skillRecord, manifest_sha256: (skillRecord.manifest_sha256 as string).toUpperCase() })),
      expect: { ok: false, code: "record_invalid", field: "manifest_sha256" },
    },
    {
      name: "duplicate_key",
      note: "a lax parser keeps the last 'name' and the signature verifies; a strict parser must refuse",
      package: "skill",
      served_text: `{"name":"@example/evil",${skillSigned.canonical.slice(1)}`,
      signature: skillSigned.signature,
      expect: { ok: false, code: "record_invalid", field: "json" },
    },
    ...[
      ["json_number_fraction", "1.0"],
      ["json_number_exponent_lower", "1e2"],
      ["json_number_exponent_upper", "1E0"],
      ["json_number_zero_exponent", "0e0"],
      ["json_number_negative_zero", "-0"],
    ].map(([name, token]): Case => ({
      name,
      note: `archive_bytes uses ${token}; strict parsing refuses it before checking the unchanged valid record's signature`,
      package: "skill",
      served_text: skillSigned.canonical.replace(/"archive_bytes":\d+/, `"archive_bytes":${token}`),
      signature: skillSigned.signature,
      expect: { ok: false, code: "record_invalid", field: "json" },
    })),
    {
      name: "embedded_key_swapped",
      note: "record names key 2 but was signed by key 1",
      package: "skill",
      ...served(signed({ ...skillRecord, public_key: TEST_KEY_2.publicKeyHex }, TEST_KEY_1)),
      expect: { ok: false, code: "signature_invalid" },
    },
    {
      name: "untrusted_key",
      note: "valid signature by key 2, which the verifier does not trust for this scope",
      package: "skill",
      ...served(signed({ ...skillRecord, public_key: TEST_KEY_2.publicKeyHex }, TEST_KEY_2)),
      expect: { ok: false, code: "key_unknown" },
    },
    { name: "name_mismatch", note: "signed record names another package", package: "skill", ...served(signed({ ...skillRecord, name: "@example/other-skill" })), expect: { ok: false, code: "record_mismatch", field: "name" } },
    { name: "version_mismatch", note: "signed record names another version", package: "skill", ...served(signed({ ...skillRecord, version: "1.0.1" })), expect: { ok: false, code: "record_mismatch", field: "version" } },
    {
      name: "manifest_digest_mismatch",
      note: "manifest_sha256 is not the digest of the served ck.json",
      package: "skill",
      ...served(signed({ ...skillRecord, manifest_sha256: sha256Hex("not the manifest") })),
      expect: { ok: false, code: "record_mismatch", field: "manifest_sha256" },
    },
    {
      name: "archive_digest_mismatch",
      note: "archive_sha256 is not the digest of the served archive",
      package: "skill",
      ...served(signed({ ...skillRecord, archive_sha256: sha256Hex("not the archive") })),
      expect: { ok: false, code: "record_mismatch", field: "archive_sha256" },
    },
    {
      name: "archive_bytes_mismatch",
      note: "archive_bytes is one more than the served archive's length",
      package: "skill",
      ...served(signed({ ...skillRecord, archive_bytes: (skillRecord.archive_bytes as number) + 1 })),
      expect: { ok: false, code: "record_mismatch", field: "archive_bytes" },
    },
    { name: "flow_version_on_skill", note: "flow_version is present only for kind flow", package: "skill", ...served(signed({ ...skillRecord, flow_version: 1 })), expect: { ok: false, code: "record_mismatch", field: "flow_version" } },
    { name: "flow_version_missing", note: "a flow record must carry flow.json's integer version", package: "flow", ...served(signed(without(flowRecord, "flow_version"))), expect: { ok: false, code: "record_mismatch", field: "flow_version" } },
    { name: "flow_version_mismatch", note: "flow.json says 3, the record says 2", package: "flow", ...served(signed({ ...flowRecord, flow_version: 2 })), expect: { ok: false, code: "record_mismatch", field: "flow_version" } },
  ]
  for (const c of cases) {
    const got = verifyRecord(c.served_text, c.signature, packages[c.package], trusted)
    if (JSON.stringify(got) !== JSON.stringify(c.expect)) throw new Error(`record case ${c.name}: got ${JSON.stringify(got)}`)
  }
  // The lax-parser trap must really be a trap: with last-wins parsing the
  // duplicate-key text canonicalizes to the signed bytes.
  if (jcs(JSON.parse(cases.find((c) => c.name === "duplicate_key")!.served_text)) !== skillSigned.canonical) throw new Error("duplicate_key trap")

  return {
    schema: "ck-catalog-v1/record",
    signing: {
      domain_tag: DOMAIN_RECORD,
      signing_input: "ASCII domain tag, one 0x00 byte, then the UTF-8 JCS bytes of the parsed record",
      algorithm: "Ed25519 (RFC 8032, pure, deterministic)",
      encodings: "digests, public keys and signatures are lowercase hex; key fingerprint is SHA-256 of the 32 raw public key bytes",
      verification_order: [
        "record_invalid (strict parse, including duplicate keys and fraction, exponent or -0 number tokens; exact field set; field formats)",
        "signature_invalid",
        "key_unknown",
        "record_mismatch (manifest_sha256, archive_sha256, archive_bytes)",
        "archive_invalid (the strict ustar reader of archive.json; field names the rule)",
        "record_mismatch (ck.json member, name, version, flow_version, flow.json id and version)",
        "files_mismatch",
      ],
    },
    keys: { trusted: keyOut(TEST_KEY_1), untrusted: keyOut(TEST_KEY_2) },
    jcs: jcsVectors(),
    packages: { skill: packageOut(SKILL_PACKAGE), flow: packageOut(FLOW_PACKAGE) },
    valid,
    cases,
  }

  function served(s: { canonical: string; signature: string }) {
    return { served_text: s.canonical, signature: s.signature }
  }
}

// ───────────────────────────── ck-status/v1 ─────────────────────────────

const STATUS_FIELDS = ["schema", "registry_key_id", "name", "version", "archive_sha256", "publisher_key", "sequence", "state", "reason", "compromise_from", "published_at", "changed_at"]
const STATES = ["available", "withdrawn", "tombstoned", "revoked"] as const
type State = (typeof STATES)[number]
// RFC 3339 in UTC with whole seconds and a literal Z, so that string order is time order.
const TIMESTAMP = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$/
const TEST_REGISTRY_KEY_ID = "test-registry-1"

// Which state may follow which. Withdrawal is reversible; a tombstone is
// final; a revoked version can only be tombstoned afterwards.
const ALLOWED_NEXT: Record<State, State[]> = {
  available: ["available", "withdrawn", "tombstoned", "revoked"],
  withdrawn: ["available", "withdrawn", "tombstoned", "revoked"],
  revoked: ["revoked", "tombstoned"],
  tombstoned: ["tombstoned"],
}

export function statusRecord(pkg: VectorPackage, fields: { sequence: number; state: State; reason: string | null; compromise_from?: string | null; published_at: string; changed_at: string }) {
  return {
    schema: "ck-status/v1",
    registry_key_id: TEST_REGISTRY_KEY_ID,
    name: pkg.manifest.name as string,
    version: pkg.manifest.version as string,
    archive_sha256: sha256Hex(pkg.archive),
    publisher_key: TEST_KEY_1.fingerprint,
    sequence: fields.sequence,
    state: fields.state,
    reason: fields.reason,
    compromise_from: fields.compromise_from ?? null,
    published_at: fields.published_at,
    changed_at: fields.changed_at,
  }
}

export type ApplyResult = { ok: true; result: "applied" | "duplicate" } | { ok: false; code: string; field?: string; reason?: string }

// The client's reference status applier: shape, registry key, signature, the
// pinned version it is about, then ordering against the record already held.
export function applyStatus(
  held: string | null,
  incomingText: string,
  signatureHex: string,
  registryKeys: Record<string, string>,
  pinned: { name: string; version: string; archive_sha256: string },
): ApplyResult {
  let parsed: unknown
  try {
    parsed = parseStrictJson(incomingText)
  } catch {
    return { ok: false, code: "status_invalid", field: "json" }
  }
  if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed)) return { ok: false, code: "status_invalid", field: "json" }
  const s = parsed as Record<string, unknown>
  for (const key of Object.keys(s)) if (!STATUS_FIELDS.includes(key)) return { ok: false, code: "status_invalid", field: key }
  for (const key of STATUS_FIELDS) if (!Object.hasOwn(s, key)) return { ok: false, code: "status_invalid", field: key }
  if (s.schema !== "ck-status/v1") return { ok: false, code: "status_invalid", field: "schema" }
  if (!STATES.includes(s.state as State)) return { ok: false, code: "status_invalid", field: "state" }
  if (!Number.isSafeInteger(s.sequence) || (s.sequence as number) < 1) return { ok: false, code: "status_invalid", field: "sequence" }
  if (s.state === "available" ? !(s.reason === null || typeof s.reason === "string") : !(typeof s.reason === "string" && s.reason.length > 0)) {
    return { ok: false, code: "status_invalid", field: "reason" }
  }
  if (s.state === "revoked" ? !(typeof s.compromise_from === "string" && TIMESTAMP.test(s.compromise_from)) : s.compromise_from !== null) {
    return { ok: false, code: "status_invalid", field: "compromise_from" }
  }
  for (const key of ["published_at", "changed_at"]) if (typeof s[key] !== "string" || !TIMESTAMP.test(s[key] as string)) return { ok: false, code: "status_invalid", field: key }
  if (!isLowerHex(s.archive_sha256, 64) || !isLowerHex(s.publisher_key, 64)) return { ok: false, code: "status_invalid", field: "digest" }
  const registryKey = registryKeys[s.registry_key_id as string]
  if (!registryKey) return { ok: false, code: "key_unknown" }
  const canonical = jcs(s)
  if (!verifyHex(registryKey, signingInput(DOMAIN_STATUS, canonical), signatureHex)) return { ok: false, code: "signature_invalid" }
  if (s.name !== pinned.name || s.version !== pinned.version || s.archive_sha256 !== pinned.archive_sha256) return { ok: false, code: "record_mismatch" }
  if (held === null) return { ok: true, result: "applied" }
  const prior = JSON.parse(held) as Record<string, unknown>
  // Never go backwards: a lower sequence, or the same sequence with other
  // bytes, is a rollback (a stale cache, a replay or an equivocating
  // registry) and is refused as feed_rollback; the caller logs every such
  // refusal.
  if ((s.sequence as number) < (prior.sequence as number)) return { ok: false, code: "feed_rollback", reason: "lower_sequence" }
  if (s.sequence === prior.sequence) return canonical === held ? { ok: true, result: "duplicate" } : { ok: false, code: "feed_rollback", reason: "same_sequence_other_bytes" }
  if (!ALLOWED_NEXT[prior.state as State].includes(s.state as State)) return { ok: false, code: "status_terminal" }
  return { ok: true, result: "applied" }
}

// Effects on a client, per kind and state. "running" is what happens to an
// agent that already runs the version: sessions for prompt-bearing kinds, runs
// for flows. A flow's in-flight activation settles normally; Basal refuses
// every later dispatch once core has called flow.install_revoked.
export function statusEffect(kind: string, state: State) {
  const blocked = state === "tombstoned" || state === "revoked"
  return {
    kind,
    state,
    user_notice: state === "available" ? "none" : blocked ? "blocking" : "notice",
    resolution: state === "available" ? "eligible" : "skipped",
    exact_fetch: state === "tombstoned" ? "gone" : "allowed",
    activation: blocked ? "blocked" : "allowed",
    running: !blocked ? "continues" : kind === "flow" ? "runs_stop_at_next_activation" : "sessions_get_blocking_notice",
    core_flow_revocation: blocked && kind === "flow",
    scripts_may_start: kind === "skill" ? !blocked : null,
  }
}

// ───────────────────────────── the status feed ─────────────────────────────

// One status write in a scope, numbered by the scope-wide feed_seq.
export interface FeedWrite {
  feed_seq: number
  id: string
  record: ReturnType<typeof statusRecord>
}

export type FeedReply = { status: 200; feed_seq: number; records: FeedWrite[] } | { status: 410; code: "feed_after_unavailable" }

// The registry's feed: the latest status record of every version, in
// feed_seq order. With ?after=N only versions whose latest record is newer
// than N appear, so a version's intermediate records may be skipped (status
// sequences allow gaps). An `after` above the current feed_seq (a stale edge
// copy or a registry rollback) or below the oldest point the registry still
// serves is answered 410 feed_after_unavailable; nothing newer is a 200 with
// no records.
export function feedReply(log: FeedWrite[], oldestServableAfter: number, after: number | null): FeedReply {
  const current = log.reduce((max, w) => Math.max(max, w.feed_seq), 0)
  if (after !== null && (after > current || after < oldestServableAfter)) return { status: 410, code: "feed_after_unavailable" }
  const latest = new Map<string, FeedWrite>()
  for (const w of log) {
    const key = `${w.record.name}@${w.record.version}`
    if ((latest.get(key)?.feed_seq ?? 0) < w.feed_seq) latest.set(key, w)
  }
  const records = [...latest.values()].filter((w) => w.feed_seq > (after ?? 0)).sort((a, b) => a.feed_seq - b.feed_seq)
  return { status: 200, feed_seq: current, records }
}

export interface FeedClientState {
  feed_seq: number | null
  // name@version → JCS text of the status record held for it
  held: Record<string, string>
}

export interface FeedOutcome {
  refusal: { code: string; field?: string } | null
  records: { id: string; result: ApplyResult }[]
  next_state: FeedClientState
  next_step: "done" | "fetch_full_feed"
}

// The client side of a poll. The feed's own feed_seq must never go below the
// highest one applied for the scope, and every record goes through the
// per-version monotonic check of applyStatus. The client advances its
// feed_seq only when every record applied or was a duplicate. A rollback or
// an unservable ?after= in an incremental poll falls back to the full feed;
// a rollback in the full feed itself is refused and logged.
export function applyFeed(
  state: FeedClientState,
  mode: "full" | "after",
  reply: { status: 200; feed_seq: number; records: { id: string; record_text: string; signature: string }[] } | { status: 410; code: string },
  registryKeys: Record<string, string>,
  pins: Record<string, { name: string; version: string; archive_sha256: string }>,
): FeedOutcome {
  const fallback = mode === "after" ? "fetch_full_feed" : "done"
  if (reply.status === 410) return { refusal: { code: reply.code }, records: [], next_state: state, next_step: fallback }
  if (state.feed_seq !== null && reply.feed_seq < state.feed_seq) {
    return { refusal: { code: "feed_rollback", field: "feed_seq" }, records: [], next_state: state, next_step: fallback }
  }
  const held = { ...state.held }
  const results: { id: string; result: ApplyResult }[] = []
  for (const r of reply.records) {
    const parsed = parseStrictJson(r.record_text) as { name: string; version: string }
    const key = `${parsed.name}@${parsed.version}`
    const result = applyStatus(held[key] ?? null, r.record_text, r.signature, registryKeys, pins[key])
    if (result.ok && result.result === "applied") held[key] = jcs(parseStrictJson(r.record_text))
    results.push({ id: r.id, result })
  }
  const rolledBack = results.some((r) => !r.result.ok && r.result.code === "feed_rollback")
  const allApplied = results.every((r) => r.result.ok)
  return {
    refusal: rolledBack ? { code: "feed_rollback", field: "record" } : null,
    records: results,
    next_state: { feed_seq: allApplied ? reply.feed_seq : state.feed_seq, held },
    next_step: rolledBack ? fallback : "done",
  }
}

function statusVectors() {
  const registryKeys = { [TEST_REGISTRY_KEY_ID]: TEST_REGISTRY_KEY_1.publicKeyHex }
  const P = "2026-10-14T09:00:00Z"
  const s1 = statusRecord(SKILL_PACKAGE, { sequence: 1, state: "available", reason: null, published_at: P, changed_at: P })
  const s2 = statusRecord(SKILL_PACKAGE, { sequence: 2, state: "withdrawn", reason: "Withdrawn by the publisher: the script prints the wrong greeting", published_at: P, changed_at: "2026-10-15T10:00:00Z" })
  const s2b = statusRecord(SKILL_PACKAGE, { sequence: 2, state: "available", reason: null, published_at: P, changed_at: "2026-10-15T10:00:00Z" })
  const s3 = statusRecord(SKILL_PACKAGE, { sequence: 3, state: "available", reason: "Withdrawal reversed by the publisher", published_at: P, changed_at: "2026-10-16T08:00:00Z" })
  const s4 = statusRecord(SKILL_PACKAGE, { sequence: 4, state: "tombstoned", reason: "Removed by the registry: malware", published_at: P, changed_at: "2026-10-17T12:00:00Z" })
  const s5 = statusRecord(SKILL_PACKAGE, { sequence: 5, state: "available", reason: null, published_at: P, changed_at: "2026-10-18T12:00:00Z" })
  const F = "2026-10-21T09:00:00Z"
  const f1 = statusRecord(FLOW_PACKAGE, { sequence: 1, state: "available", reason: null, published_at: F, changed_at: F })
  const f2 = statusRecord(FLOW_PACKAGE, {
    sequence: 2,
    state: "revoked",
    reason: "Publisher key revoked: key compromise",
    compromise_from: "2026-10-20T00:00:00Z",
    published_at: F,
    changed_at: "2026-10-22T09:00:00Z",
  })
  const f3 = statusRecord(FLOW_PACKAGE, { sequence: 3, state: "tombstoned", reason: "Removed by the registry: malware", published_at: F, changed_at: "2026-10-23T09:00:00Z" })

  const sig = (record: unknown, domain = DOMAIN_STATUS, key = TEST_REGISTRY_KEY_1) => signCanonical(record, key, domain)
  const records = { s1, s2, s2b, s3, s4, f1, f2, f3 }
  const signedRecords = Object.entries(records).map(([id, record]) => {
    const { canonical, signature } = sig(record)
    return { id, record, canonical, canonical_sha256: sha256Hex(canonical), signing_input_hex: signingInput(DOMAIN_STATUS, canonical).toString("hex"), signature }
  })
  const skillPin = { name: "@example/hello-skill", version: "1.0.0", archive_sha256: sha256Hex(SKILL_PACKAGE.archive) }
  const flowPin = { name: "@example/hello-flow", version: "1.0.0", archive_sha256: sha256Hex(FLOW_PACKAGE.archive) }
  type Case = { name: string; pinned: typeof skillPin; held: string | null; incoming_text: string; signature: string; expect: ApplyResult }
  const c = (name: string, pinned: typeof skillPin, held: unknown, incoming: unknown, expect: ApplyResult, override?: { signature?: string; domain?: string; key?: TestKey }): Case => {
    const s = sig(incoming, override?.domain, override?.key)
    return { name, pinned, held: held === null ? null : jcs(held), incoming_text: s.canonical, signature: override?.signature ?? s.signature, expect }
  }
  const cases: Case[] = [
    c("first_record", skillPin, null, s1, { ok: true, result: "applied" }),
    c("withdraw", skillPin, s1, s2, { ok: true, result: "applied" }),
    c("reverse_withdrawal", skillPin, s2, s3, { ok: true, result: "applied" }),
    c("sequence_gap_allowed", skillPin, s1, s4, { ok: true, result: "applied" }),
    c("lower_sequence_refused", skillPin, s3, s2, { ok: false, code: "feed_rollback", reason: "lower_sequence" }),
    c("same_record_again", skillPin, s2, s2, { ok: true, result: "duplicate" }),
    c("same_sequence_other_bytes", skillPin, s2, s2b, { ok: false, code: "feed_rollback", reason: "same_sequence_other_bytes" }),
    c("leave_tombstone_refused", skillPin, s4, s5, { ok: false, code: "status_terminal" }),
    c("revoked_then_tombstoned", flowPin, f2, f3, { ok: true, result: "applied" }),
    c("revoked_back_to_available_refused", flowPin, f2, { ...f1, sequence: 4, changed_at: "2026-10-24T09:00:00Z" }, { ok: false, code: "status_terminal" }),
    c("bad_signature", skillPin, s1, s2, { ok: false, code: "signature_invalid" }, { signature: flipFirstBit(sig(s2).signature) }),
    c("record_domain_signature", skillPin, s1, s2, { ok: false, code: "signature_invalid" }, { domain: DOMAIN_RECORD }),
    c("signed_by_publisher_key", skillPin, s1, s2, { ok: false, code: "signature_invalid" }, { key: TEST_KEY_1 }),
    c("unknown_registry_key", skillPin, s1, { ...s2, registry_key_id: "test-registry-9" }, { ok: false, code: "key_unknown" }),
    c("other_archive", skillPin, s1, { ...s2, archive_sha256: sha256Hex(FLOW_PACKAGE.archive) }, { ok: false, code: "record_mismatch" }),
    c("revoked_without_compromise_from", flowPin, f1, { ...f2, compromise_from: null }, { ok: false, code: "status_invalid", field: "compromise_from" }),
    c("withdrawn_without_reason", skillPin, s1, { ...s2, reason: null }, { ok: false, code: "status_invalid", field: "reason" }),
    c("timestamp_with_offset", skillPin, s1, { ...s2, changed_at: "2026-10-15T12:00:00+02:00" }, { ok: false, code: "status_invalid", field: "changed_at" }),
  ]
  for (const k of cases) {
    const got = applyStatus(k.held, k.incoming_text, k.signature, registryKeys, k.pinned)
    if (JSON.stringify(got) !== JSON.stringify(k.expect)) throw new Error(`status case ${k.name}: got ${JSON.stringify(got)}`)
  }

  // Key revocation blocks a version when its registry-attested publish time
  // is at or after the compromise-from time.
  const revocation = [
    { compromise_from: "2026-10-20T00:00:00Z", published_at: "2026-10-19T23:59:59Z", blocked: false },
    { compromise_from: "2026-10-20T00:00:00Z", published_at: "2026-10-20T00:00:00Z", blocked: true },
    { compromise_from: "2026-10-20T00:00:00Z", published_at: "2026-10-21T09:00:00Z", blocked: true },
  ]
  for (const r of revocation) if ((r.published_at >= r.compromise_from) !== r.blocked) throw new Error("revocation table")

  const effects = ["persona", "skill", "flow", "memory-profile"].flatMap((kind) => STATES.map((state) => statusEffect(kind, state)))
  const withdrawnFlow = effects.find((e) => e.kind === "flow" && e.state === "withdrawn")
  if (withdrawnFlow?.core_flow_revocation !== false || withdrawnFlow.activation !== "allowed") throw new Error("withdrawal must not revoke flows")

  return {
    schema: "ck-catalog-v1/status",
    signing: { domain_tag: DOMAIN_STATUS, signing_input: "ASCII domain tag, one 0x00 byte, then the UTF-8 JCS bytes of the parsed status record", algorithm: "Ed25519" },
    registry_keys: {
      NOTE: "Status records are signed by the TEST-ONLY registry status key, never by a publisher key; the signed_by_publisher_key case shows a publisher signature refused. record_domain_signature shows the registry key's own signature refused under the ck-record/v1 tag.",
      [TEST_REGISTRY_KEY_ID]: keyOut(TEST_REGISTRY_KEY_1),
    },
    allowed_next_state: ALLOWED_NEXT,
    records: signedRecords,
    apply_cases: cases,
    feed: feedVectors(),
    revocation_time: revocation,
    effects,
  }

  function feedVectors() {
    // Every status write in scope @example, in order. f2 is skipped by an
    // ?after=5 poll at feed_seq 7 because f3 supersedes it.
    const log: FeedWrite[] = [
      { feed_seq: 1, id: "s1", record: s1 },
      { feed_seq: 2, id: "s2", record: s2 },
      { feed_seq: 3, id: "s3", record: s3 },
      { feed_seq: 4, id: "s4", record: s4 },
      { feed_seq: 5, id: "f1", record: f1 },
      { feed_seq: 6, id: "f2", record: f2 },
      { feed_seq: 7, id: "f3", record: f3 },
    ]
    const oldestServableAfter = 2
    type View = { name: string; note: string; log_up_to: number; after: number | null; expect: { status: 200; feed_seq: number; records: { feed_seq: number; id: string }[] } | { status: 410; code: "feed_after_unavailable" } }
    const views: View[] = [
      { name: "full_at_5", note: "the default: latest record of every version", log_up_to: 5, after: null, expect: { status: 200, feed_seq: 5, records: [{ feed_seq: 4, id: "s4" }, { feed_seq: 5, id: "f1" }] } },
      { name: "after_3_at_5", note: "", log_up_to: 5, after: 3, expect: { status: 200, feed_seq: 5, records: [{ feed_seq: 4, id: "s4" }, { feed_seq: 5, id: "f1" }] } },
      { name: "after_4_at_5", note: "", log_up_to: 5, after: 4, expect: { status: 200, feed_seq: 5, records: [{ feed_seq: 5, id: "f1" }] } },
      { name: "nothing_newer", note: "200 with no records and the current feed_seq, never 304 (304 answers only If-None-Match)", log_up_to: 5, after: 5, expect: { status: 200, feed_seq: 5, records: [] } },
      { name: "after_above_current", note: "a stale edge copy or a registry rollback; the registry does not tell them apart", log_up_to: 5, after: 6, expect: { status: 410, code: "feed_after_unavailable" } },
      { name: "after_below_kept_history", note: "this registry serves ?after= only from 2 on", log_up_to: 5, after: 1, expect: { status: 410, code: "feed_after_unavailable" } },
      { name: "after_5_at_7", note: "f2 (feed_seq 6) is skipped: f3 is the flow version's latest record", log_up_to: 7, after: 5, expect: { status: 200, feed_seq: 7, records: [{ feed_seq: 7, id: "f3" }] } },
      { name: "full_at_7", note: "", log_up_to: 7, after: null, expect: { status: 200, feed_seq: 7, records: [{ feed_seq: 4, id: "s4" }, { feed_seq: 7, id: "f3" }] } },
    ]
    const byId = Object.fromEntries(signedRecords.map((r) => [r.id, r]))
    const body = (reply: FeedReply) =>
      reply.status === 410
        ? { status: 410, body: { error: { code: reply.code } } }
        : { status: 200, body: { schema: "ck-status-feed/v1", scope: "example", feed_seq: reply.feed_seq, records: reply.records.map((w) => ({ feed_seq: w.feed_seq, record: w.record, signature: byId[w.id].signature })) } }
    const viewOut = views.map((v) => {
      const got = feedReply(log.filter((w) => w.feed_seq <= v.log_up_to), oldestServableAfter, v.after)
      const summary = got.status === 410 ? got : { status: got.status, feed_seq: got.feed_seq, records: got.records.map((w) => ({ feed_seq: w.feed_seq, id: w.id })) }
      if (jcs(summary) !== jcs(v.expect)) throw new Error(`feed view ${v.name}: got ${JSON.stringify(summary)}`)
      return { ...v, request: `GET /v1/scopes/example/status${v.after === null ? "" : `?after=${v.after}`}` }
    })
    const exampleReply = body(feedReply(log, oldestServableAfter, 5))

    // Client polls. A reply is given as served: records with their feed_seq
    // and signature; `ids` name the signed records above.
    const pins = { "@example/hello-skill@1.0.0": skillPin, "@example/hello-flow@1.0.0": flowPin }
    const heldOf = (...ids: string[]) => Object.fromEntries(ids.map((id) => [`${byId[id].record.name}@${byId[id].record.version}`, byId[id].canonical]))
    const reply200 = (feed_seq: number, ids: string[]) => ({ status: 200 as const, feed_seq, records: ids.map((id) => ({ id, record_text: byId[id].canonical, signature: byId[id].signature })) })
    type ClientCase = { name: string; note: string; state: FeedClientState; mode: "full" | "after"; reply: ReturnType<typeof reply200> | { status: 410; code: string }; expect: FeedOutcome }
    const ok = (id: string, result: "applied" | "duplicate" = "applied") => ({ id, result: { ok: true as const, result } })
    const clientCases: ClientCase[] = [
      {
        name: "first_fetch_is_full",
        note: "no feed_seq held for the scope yet, so the client fetches the full feed",
        state: { feed_seq: null, held: {} },
        mode: "full",
        reply: reply200(5, ["s4", "f1"]),
        expect: { refusal: null, records: [ok("s4"), ok("f1")], next_state: { feed_seq: 5, held: heldOf("s4", "f1") }, next_step: "done" },
      },
      {
        name: "incremental_poll",
        note: "?after=5 at feed_seq 7",
        state: { feed_seq: 5, held: heldOf("s4", "f1") },
        mode: "after",
        reply: reply200(7, ["f3"]),
        expect: { refusal: null, records: [ok("f3")], next_state: { feed_seq: 7, held: heldOf("s4", "f3") }, next_step: "done" },
      },
      {
        name: "nothing_newer",
        note: "",
        state: { feed_seq: 7, held: heldOf("s4", "f3") },
        mode: "after",
        reply: reply200(7, []),
        expect: { refusal: null, records: [], next_state: { feed_seq: 7, held: heldOf("s4", "f3") }, next_step: "done" },
      },
      {
        name: "feed_seq_rollback_incremental",
        note: "a stale copy answers with feed_seq 5 after the client applied 7: refused, nothing applied, fall back to the full feed",
        state: { feed_seq: 7, held: heldOf("s4", "f3") },
        mode: "after",
        reply: reply200(5, ["f1"]),
        expect: { refusal: { code: "feed_rollback", field: "feed_seq" }, records: [], next_state: { feed_seq: 7, held: heldOf("s4", "f3") }, next_step: "fetch_full_feed" },
      },
      {
        name: "feed_seq_rollback_full",
        note: "the full feed itself is behind: refused and logged; the client keeps what it holds",
        state: { feed_seq: 7, held: heldOf("s4", "f3") },
        mode: "full",
        reply: reply200(5, ["s4", "f1"]),
        expect: { refusal: { code: "feed_rollback", field: "feed_seq" }, records: [], next_state: { feed_seq: 7, held: heldOf("s4", "f3") }, next_step: "done" },
      },
      {
        name: "record_rollback",
        note: "a reply whose feed_seq is newer but which carries a lower status sequence than the one applied (s2 after s3)",
        state: { feed_seq: 3, held: heldOf("s3") },
        mode: "after",
        reply: reply200(6, ["s2"]),
        expect: {
          refusal: { code: "feed_rollback", field: "record" },
          records: [{ id: "s2", result: { ok: false, code: "feed_rollback", reason: "lower_sequence" } }],
          next_state: { feed_seq: 3, held: heldOf("s3") },
          next_step: "fetch_full_feed",
        },
      },
      {
        name: "same_sequence_other_bytes",
        note: "s2b has s2's sequence with other bytes: an equivocating registry or a forged copy",
        state: { feed_seq: 2, held: heldOf("s2") },
        mode: "after",
        reply: reply200(4, ["s2b"]),
        expect: {
          refusal: { code: "feed_rollback", field: "record" },
          records: [{ id: "s2b", result: { ok: false, code: "feed_rollback", reason: "same_sequence_other_bytes" } }],
          next_state: { feed_seq: 2, held: heldOf("s2") },
          next_step: "fetch_full_feed",
        },
      },
      {
        name: "after_unavailable",
        note: "410 feed_after_unavailable: fall back to the full feed",
        state: { feed_seq: 1, held: heldOf("s1") },
        mode: "after",
        reply: { status: 410, code: "feed_after_unavailable" },
        expect: { refusal: { code: "feed_after_unavailable" }, records: [], next_state: { feed_seq: 1, held: heldOf("s1") }, next_step: "fetch_full_feed" },
      },
      {
        name: "full_feed_fallback",
        note: "the fallback fetch: applied through the same monotonic checks",
        state: { feed_seq: 1, held: heldOf("s1") },
        mode: "full",
        reply: reply200(5, ["s4", "f1"]),
        expect: { refusal: null, records: [ok("s4"), ok("f1")], next_state: { feed_seq: 5, held: heldOf("s4", "f1") }, next_step: "done" },
      },
    ]
    for (const k of clientCases) {
      const got = applyFeed(k.state, k.mode, k.reply, registryKeys, pins)
      if (jcs(got) !== jcs(k.expect)) throw new Error(`feed client case ${k.name}: got ${JSON.stringify(got)}`)
    }
    return {
      rules: {
        feed_seq: "scope-wide, strictly increasing, one per status write in the scope; unsigned feed metadata",
        full_feed: "GET /v1/scopes/{scope}/status: the latest record of every version in the scope, ordered by feed_seq; the reply's feed_seq is the scope's current one",
        after: "?after=N: only versions whose latest record has feed_seq > N; 410 feed_after_unavailable when N is above the current feed_seq or below the oldest point the registry serves; 200 with no records when nothing is newer",
        client: "keep the highest feed_seq applied per scope and the highest status sequence per version; a lower feed_seq, a lower sequence, or the same sequence with other bytes is feed_rollback; advance feed_seq only when every record applied or was a duplicate; first fetch, feed_rollback and feed_after_unavailable in an incremental poll fall back to the full feed",
      },
      oldest_servable_after: oldestServableAfter,
      log: log.map((w) => ({ feed_seq: w.feed_seq, id: w.id })),
      views: viewOut,
      example_reply: { request: "GET /v1/scopes/example/status?after=5", ...exampleReply },
      client_cases: clientCases,
    }
  }
}

// ───────────────────────────── key and status statements ─────────────────────────────

// Every statement is signed like a record: its schema string is the domain
// tag, then one 0x00 byte, then the JCS bytes of the parsed statement.
export type StatementResult = { ok: true; result: string; next?: { sequence: number; state: State } } | { ok: false; code: string; field?: string }

function parseStatement(text: string, schema: string, fields: string[]): { ok: true; s: Record<string, unknown> } | { ok: false; code: "statement_invalid"; field: string } {
  let parsed: unknown
  try {
    parsed = parseStrictJson(text)
  } catch {
    return { ok: false, code: "statement_invalid", field: "json" }
  }
  if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed)) return { ok: false, code: "statement_invalid", field: "json" }
  const s = parsed as Record<string, unknown>
  for (const key of Object.keys(s)) if (!fields.includes(key)) return { ok: false, code: "statement_invalid", field: key }
  for (const key of fields) if (!Object.hasOwn(s, key)) return { ok: false, code: "statement_invalid", field: key }
  if (s.schema !== schema) return { ok: false, code: "statement_invalid", field: "schema" }
  return { ok: true, s }
}

const isTime = (value: unknown): value is string => typeof value === "string" && TIMESTAMP.test(value)
const invalid = (field: string): StatementResult => ({ ok: false, code: "statement_invalid", field })

// A scope's key set as the registry serves it. A key is current while it has
// no valid_until and no revocation.
export interface ScopeKey {
  public_key: string
  valid_from: string
  valid_until: string | null
  revocation: { reason: string; compromise_from: string } | null
}

function currentKeyCheck(keys: ScopeKey[], publicKey: string): StatementResult | null {
  const key = keys.find((k) => k.public_key === publicKey)
  if (!key) return { ok: false, code: "key_unknown" }
  if (key.revocation) return { ok: false, code: "key_revoked" }
  if (key.valid_until !== null) return { ok: false, code: "key_not_valid" }
  return null
}

// ck-registry-key/v1: the registry introduces its next status key, signed by
// the newest key the client already holds for that registry. The client's
// key set starts from the key pinned with the allowlist entry and
// grows only along this chain; old keys are kept for records already held.
// Only that first key needs the user's approval on their device; chain successors
// are pinned without it.
export interface RegistryPin {
  registry: string
  keys: { key_id: string; public_key: string; statement: string | null }[]
}

export function applyRegistryKey(pin: RegistryPin, text: string, signatureHex: string): StatementResult {
  const p = parseStatement(text, DOMAIN_REGISTRY_KEY, ["schema", "registry", "key_id", "public_key", "valid_from"])
  if (!p.ok) return p
  const s = p.s
  if (typeof s.registry !== "string" || s.registry === "") return invalid("registry")
  if (typeof s.key_id !== "string" || s.key_id === "") return invalid("key_id")
  if (!isLowerHex(s.public_key, 64)) return invalid("public_key")
  if (!isTime(s.valid_from)) return invalid("valid_from")
  if (s.registry !== pin.registry) return { ok: false, code: "record_mismatch", field: "registry" }
  const canonical = jcs(s)
  // The same bytes as a statement already accepted are a no-op; other bytes
  // under a held key id or for a held public key are a conflict.
  const sameId = pin.keys.find((k) => k.key_id === s.key_id)
  if (sameId) return sameId.statement === canonical ? { ok: true, result: "duplicate" } : { ok: false, code: "key_conflict", field: "key_id" }
  if (pin.keys.some((k) => k.public_key === s.public_key)) return { ok: false, code: "key_conflict", field: "public_key" }
  const current = pin.keys[pin.keys.length - 1]
  if (!verifyHex(current.public_key, signingInput(DOMAIN_REGISTRY_KEY, canonical), signatureHex)) return { ok: false, code: "signature_invalid" }
  return { ok: true, result: "accepted" }
}

// ck-key-rotation/v1: a third-party scope's routine key rotation, signed by
// both keys. The old key's signature authorizes it; the new key's proves
// possession. The registry assigns valid_from, and a key that was ever in
// any scope's set is never added again.
export function verifyKeyRotation(text: string, oldSignature: string, newSignature: string, scope: string, keys: ScopeKey[], everInAnyScope: string[]): StatementResult {
  const p = parseStatement(text, DOMAIN_KEY_ROTATION, ["schema", "scope", "old_key", "new_key"])
  if (!p.ok) return p
  const s = p.s
  if (typeof s.scope !== "string" || !NAME_PART.test(s.scope) || s.scope.length > SCOPE_MAX) return invalid("scope")
  if (!isLowerHex(s.old_key, 64)) return invalid("old_key")
  if (!isLowerHex(s.new_key, 64) || s.new_key === s.old_key) return invalid("new_key")
  if (s.scope !== scope) return { ok: false, code: "record_mismatch", field: "scope" }
  const input = signingInput(DOMAIN_KEY_ROTATION, jcs(s))
  if (!verifyHex(s.old_key, input, oldSignature)) return { ok: false, code: "signature_invalid", field: "old_signature" }
  if (!verifyHex(s.new_key, input, newSignature)) return { ok: false, code: "signature_invalid", field: "new_signature" }
  const keyCheck = currentKeyCheck(keys, s.old_key)
  if (keyCheck) return keyCheck
  if (everInAnyScope.includes(s.new_key)) return { ok: false, code: "key_reused" }
  return { ok: true, result: "accepted" }
}

// ck-key-cert/v1: the offline @cortexkit root certifies one @cortexkit publish
// key for a window. Rotating a @cortexkit publish key is a new cert.
export function verifyKeyCert(text: string, signatureHex: string, rootPublicKey: string): StatementResult {
  const p = parseStatement(text, DOMAIN_KEY_CERT, ["schema", "scope", "public_key", "valid_from", "valid_until"])
  if (!p.ok) return p
  const s = p.s
  if (s.scope !== "cortexkit") return invalid("scope")
  if (!isLowerHex(s.public_key, 64)) return invalid("public_key")
  if (!isTime(s.valid_from)) return invalid("valid_from")
  if (!(s.valid_until === null || (isTime(s.valid_until) && s.valid_until > s.valid_from))) return invalid("valid_until")
  if (!verifyHex(rootPublicKey, signingInput(DOMAIN_KEY_CERT, jcs(s)), signatureHex)) return { ok: false, code: "signature_invalid" }
  return { ok: true, result: "certified" }
}

// Key windows are half open: valid_from is inside, valid_until is not.
export function windowCovers(window: { valid_from: string; valid_until: string | null }, publishedAt: string): boolean {
  return publishedAt >= window.valid_from && (window.valid_until === null || publishedAt < window.valid_until)
}

// ck-status-request/v1: a publisher's withdraw or unwithdraw, signed by a
// current key of the package's scope. after_sequence must equal the
// version's current status sequence: the registry applies it as one
// compare-and-set, so a replayed or stale request changes nothing.
export interface VersionStatus {
  archive_sha256: string
  sequence: number
  state: State
}

export function applyStatusRequest(
  path: { action: string; name: string; version: string },
  request: { statement_text: string; public_key: string; signature: string },
  current: VersionStatus,
  keys: ScopeKey[],
): StatementResult {
  const p = parseStatement(request.statement_text, DOMAIN_STATUS_REQUEST, ["schema", "action", "name", "version", "archive_sha256", "reason", "after_sequence"])
  if (!p.ok) return p
  const s = p.s
  if (s.action !== "withdraw" && s.action !== "unwithdraw") return invalid("action")
  const mapped = typeof s.name === "string" ? mapName(s.name) : null
  if (!mapped?.ok || mapped.canonical !== s.name) return invalid("name")
  if (typeof s.version !== "string" || !parseVersion(s.version)) return invalid("version")
  if (!isLowerHex(s.archive_sha256, 64)) return invalid("archive_sha256")
  const reasonOk = typeof s.reason === "string" ? s.reason.length > 0 : s.reason === null && s.action === "unwithdraw"
  if (!reasonOk) return invalid("reason")
  if (!Number.isSafeInteger(s.after_sequence) || (s.after_sequence as number) < 1) return invalid("after_sequence")
  for (const field of ["action", "name", "version"] as const) if (s[field] !== path[field]) return { ok: false, code: "record_mismatch", field }
  if (!verifyHex(request.public_key, signingInput(DOMAIN_STATUS_REQUEST, jcs(s)), request.signature)) return { ok: false, code: "signature_invalid" }
  const keyCheck = currentKeyCheck(keys, request.public_key)
  if (keyCheck) return keyCheck
  if (s.archive_sha256 !== current.archive_sha256) return { ok: false, code: "record_mismatch", field: "archive_sha256" }
  if (s.after_sequence !== current.sequence) return { ok: false, code: "status_sequence_conflict" }
  if (s.action === "withdraw" ? current.state !== "available" : current.state !== "withdrawn") return { ok: false, code: "action_not_allowed" }
  return { ok: true, result: "applied", next: { sequence: current.sequence + 1, state: s.action === "withdraw" ? "withdrawn" : "available" } }
}

function signedStatement(statement: Record<string, unknown>, key: TestKey, domain: string) {
  const { canonical, signature } = signCanonical(statement, key, domain)
  return { canonical, signature }
}

function statementOut(statement: Record<string, unknown>, domain: string) {
  const canonical = jcs(statement)
  return { statement, canonical, canonical_sha256: sha256Hex(canonical), signing_input_hex: signingInput(domain, canonical).toString("hex") }
}

function expectSame(label: string, got: unknown, expected: unknown) {
  if (jcs(got) !== jcs(expected)) throw new Error(`${label}: got ${JSON.stringify(got)}`)
}

function statementsVectors() {
  // ── ck-registry-key/v1 ──
  const REGISTRY = "https://registry.example.invalid"
  const R1 = TEST_REGISTRY_KEY_1
  const R2 = TEST_REGISTRY_KEY_2
  const R3 = TEST_REGISTRY_KEY_3
  const st2 = { schema: DOMAIN_REGISTRY_KEY, registry: REGISTRY, key_id: "test-registry-2", public_key: R2.publicKeyHex, valid_from: "2026-11-01T00:00:00Z" }
  const st3 = { schema: DOMAIN_REGISTRY_KEY, registry: REGISTRY, key_id: "test-registry-3", public_key: R3.publicKeyHex, valid_from: "2027-05-01T00:00:00Z" }
  const pin1: RegistryPin = { registry: REGISTRY, keys: [{ key_id: "test-registry-1", public_key: R1.publicKeyHex, statement: null }] }
  const pin2: RegistryPin = { registry: REGISTRY, keys: [...pin1.keys, { key_id: "test-registry-2", public_key: R2.publicKeyHex, statement: jcs(st2) }] }
  type RkCase = { name: string; note: string; pinned: RegistryPin; statement_text: string; signature: string; expect: StatementResult }
  const rk = (name: string, note: string, pinned: RegistryPin, statement: Record<string, unknown>, key: TestKey, expect: StatementResult, o: { domain?: string; flip?: boolean } = {}): RkCase => {
    const s = signedStatement(statement, key, o.domain ?? DOMAIN_REGISTRY_KEY)
    return { name, note, pinned, statement_text: s.canonical, signature: o.flip ? flipFirstBit(s.signature) : s.signature, expect }
  }
  const registryKeyCases: RkCase[] = [
    rk("successor_signed_by_current", "key 2 introduced by pinned key 1", pin1, st2, R1, { ok: true, result: "accepted" }),
    rk("next_link", "key 3 introduced by key 2, the newest key held", pin2, st3, R2, { ok: true, result: "accepted" }),
    rk("same_statement_again", "the chain is served whole on every fetch; links already held are no-ops", pin2, st2, R1, { ok: true, result: "duplicate" }),
    rk("skips_the_chain", "key 3 signed by key 1 while key 2 is the newest key held: only the chain counts", pin2, st3, R1, { ok: false, code: "signature_invalid" }),
    rk("self_signed", "a key cannot introduce itself", pin1, st2, R2, { ok: false, code: "signature_invalid" }),
    rk("bit_flip", "first signature byte XOR 0x01", pin1, st2, R1, { ok: false, code: "signature_invalid" }, { flip: true }),
    rk("status_domain_tag", "signed under ck-status/v1 instead of ck-registry-key/v1", pin1, st2, R1, { ok: false, code: "signature_invalid" }, { domain: DOMAIN_STATUS }),
    rk("other_registry", "a statement for another allowlisted registry", pin1, { ...st2, registry: "https://other.example.invalid" }, R1, { ok: false, code: "record_mismatch", field: "registry" }),
    rk("key_id_reused", "a held key id with another key", pin2, { ...st3, key_id: "test-registry-2" }, R2, { ok: false, code: "key_conflict", field: "key_id" }),
    rk("public_key_reused", "a held public key under a new id", pin1, { ...st2, public_key: R1.publicKeyHex }, R1, { ok: false, code: "key_conflict", field: "public_key" }),
    rk("unknown_field", "", pin1, { ...st2, deadline: "2027-01-01T00:00:00Z" }, R1, { ok: false, code: "statement_invalid", field: "deadline" }),
    rk("uppercase_public_key", "", pin1, { ...st2, public_key: R2.publicKeyHex.toUpperCase() }, R1, { ok: false, code: "statement_invalid", field: "public_key" }),
    rk("timestamp_with_offset", "", pin1, { ...st2, valid_from: "2026-11-01T01:00:00+01:00" }, R1, { ok: false, code: "statement_invalid", field: "valid_from" }),
  ]
  for (const k of registryKeyCases) expectSame(`registry key case ${k.name}`, applyRegistryKey(k.pinned, k.statement_text, k.signature), k.expect)

  // ── ck-key-rotation/v1 ──
  const K1 = TEST_KEY_1
  const K2 = TEST_KEY_2
  const K3 = TEST_KEY_3
  const current = (key: TestKey, o: Partial<ScopeKey> = {}): ScopeKey => ({ public_key: key.publicKeyHex, valid_from: "2026-01-01T00:00:00Z", valid_until: null, revocation: null, ...o })
  const rotation = { schema: DOMAIN_KEY_ROTATION, scope: "example", old_key: K1.publicKeyHex, new_key: K3.publicKeyHex }
  type RotCase = { name: string; note: string; scope: string; scope_keys: ScopeKey[]; ever_in_any_scope: string[]; served: { statement: Record<string, unknown>; old_signature: string; new_signature: string }; expect: StatementResult }
  const rot = (
    name: string,
    note: string,
    statement: Record<string, unknown>,
    signers: [TestKey, TestKey],
    expect: StatementResult,
    o: { keys?: ScopeKey[]; ever?: string[]; domain?: string; flipOld?: boolean } = {},
  ): RotCase => {
    const domain = o.domain ?? DOMAIN_KEY_ROTATION
    const oldSig = signedStatement(statement, signers[0], domain).signature
    return {
      name,
      note,
      scope: "example",
      scope_keys: o.keys ?? [current(K1)],
      ever_in_any_scope: o.ever ?? [K1.publicKeyHex],
      served: { statement, old_signature: o.flipOld ? flipFirstBit(oldSig) : oldSig, new_signature: signedStatement(statement, signers[1], domain).signature },
      expect,
    }
  }
  const rotationCases: RotCase[] = [
    rot("cross_signed", "old key authorizes, new key proves possession", rotation, [K1, K3], { ok: true, result: "accepted" }),
    rot("old_signature_bit_flip", "", rotation, [K1, K3], { ok: false, code: "signature_invalid", field: "old_signature" }, { flipOld: true }),
    rot("new_signature_by_old_key", "no proof that the submitter holds the new key", rotation, [K1, K1], { ok: false, code: "signature_invalid", field: "new_signature" }),
    rot("record_domain_tag", "both signatures made under ck-record/v1", rotation, [K1, K3], { ok: false, code: "signature_invalid", field: "old_signature" }, { domain: DOMAIN_RECORD }),
    rot("old_key_not_in_scope", "key 2 signs correctly but is not in @example's set", { ...rotation, old_key: K2.publicKeyHex }, [K2, K3], { ok: false, code: "key_unknown" }),
    rot("old_key_revoked", "a revoked key cannot hand over its scope", rotation, [K1, K3], { ok: false, code: "key_revoked" }, {
      keys: [current(K1, { revocation: { reason: "key compromise", compromise_from: "2026-10-20T00:00:00Z" } })],
    }),
    rot("old_key_retired", "only a current key can rotate", rotation, [K1, K3], { ok: false, code: "key_not_valid" }, { keys: [current(K1, { valid_until: "2026-06-01T00:00:00Z" })] }),
    rot("new_key_was_in_a_scope", "a key ever in any scope's set is never added again", rotation, [K1, K3], { ok: false, code: "key_reused" }, { ever: [K1.publicKeyHex, K3.publicKeyHex] }),
    rot("same_key", "", { ...rotation, new_key: K1.publicKeyHex }, [K1, K1], { ok: false, code: "statement_invalid", field: "new_key" }),
    rot("other_scope", "submitted for @example but names @acme", { ...rotation, scope: "acme" }, [K1, K3], { ok: false, code: "record_mismatch", field: "scope" }),
    rot("carries_valid_from", "the registry assigns valid_from; the statement may not", { ...rotation, valid_from: "2026-10-14T09:00:00Z" }, [K1, K3], { ok: false, code: "statement_invalid", field: "valid_from" }),
  ]
  for (const k of rotationCases) {
    const got = verifyKeyRotation(jcs(k.served.statement), k.served.old_signature, k.served.new_signature, k.scope, k.scope_keys, k.ever_in_any_scope)
    expectSame(`rotation case ${k.name}`, got, k.expect)
  }

  // ── ck-key-cert/v1 ──
  const ROOT = TEST_ROOT_KEY
  const C1 = TEST_CORTEXKIT_KEY_1
  const C2 = TEST_CORTEXKIT_KEY_2
  const cert1 = { schema: DOMAIN_KEY_CERT, scope: "cortexkit", public_key: C1.publicKeyHex, valid_from: "2026-10-01T00:00:00Z", valid_until: "2027-04-01T00:00:00Z" }
  // The successor overlaps its predecessor's last month: rotating a
  // @cortexkit publish key is simply a new cert.
  const cert2 = { schema: DOMAIN_KEY_CERT, scope: "cortexkit", public_key: C2.publicKeyHex, valid_from: "2027-03-01T00:00:00Z", valid_until: null }
  type CertCase = { name: string; note: string; served: { statement: Record<string, unknown>; signature: string }; expect: StatementResult }
  const cc = (name: string, note: string, statement: Record<string, unknown>, key: TestKey, expect: StatementResult, domain = DOMAIN_KEY_CERT): CertCase => ({
    name,
    note,
    served: { statement, signature: signedStatement(statement, key, domain).signature },
    expect,
  })
  const certCases: CertCase[] = [
    cc("bounded_cert", "", cert1, ROOT, { ok: true, result: "certified" }),
    cc("open_ended_cert", "valid_until null: valid until a later cert or a revocation", cert2, ROOT, { ok: true, result: "certified" }),
    cc("self_certified", "signed by the certified key itself", cert1, C1, { ok: false, code: "signature_invalid" }),
    cc("signed_by_registry_key", "the registry can never make a key trusted", cert1, R1, { ok: false, code: "signature_invalid" }),
    cc("signed_by_publisher_key", "", cert1, K1, { ok: false, code: "signature_invalid" }),
    cc("record_domain_tag", "root signature under ck-record/v1", cert1, ROOT, { ok: false, code: "signature_invalid" }, DOMAIN_RECORD),
    cc("other_scope", "the root certifies @cortexkit keys only", { ...cert1, scope: "example" }, ROOT, { ok: false, code: "statement_invalid", field: "scope" }),
    cc("empty_window", "valid_until must be later than valid_from", { ...cert1, valid_until: cert1.valid_from }, ROOT, { ok: false, code: "statement_invalid", field: "valid_until" }),
    cc("uppercase_public_key", "", { ...cert1, public_key: C1.publicKeyHex.toUpperCase() }, ROOT, { ok: false, code: "statement_invalid", field: "public_key" }),
    cc("unknown_field", "", { ...cert1, name: "@cortexkit/swe-head" }, ROOT, { ok: false, code: "statement_invalid", field: "name" }),
  ]
  for (const k of certCases) expectSame(`cert case ${k.name}`, verifyKeyCert(jcs(k.served.statement), k.served.signature, ROOT.publicKeyHex), k.expect)
  const windowCases = [
    { cert: "bounded_cert", published_at: "2026-09-30T23:59:59Z", expect: { ok: false, code: "key_not_valid" } },
    { cert: "bounded_cert", published_at: "2026-10-01T00:00:00Z", expect: { ok: true } },
    { cert: "bounded_cert", published_at: "2027-03-31T23:59:59Z", expect: { ok: true } },
    { cert: "bounded_cert", published_at: "2027-04-01T00:00:00Z", expect: { ok: false, code: "key_not_valid" } },
    { cert: "open_ended_cert", published_at: "2027-02-28T23:59:59Z", expect: { ok: false, code: "key_not_valid" } },
    { cert: "open_ended_cert", published_at: "2030-01-01T00:00:00Z", expect: { ok: true } },
  ]
  for (const w of windowCases) {
    const cert = w.cert === "bounded_cert" ? cert1 : cert2
    expectSame(`cert window ${w.published_at}`, windowCovers(cert, w.published_at) ? { ok: true } : { ok: false, code: "key_not_valid" }, w.expect)
  }

  // ── ck-status-request/v1 ──
  const skillArchive = sha256Hex(SKILL_PACKAGE.archive)
  const withdraw = {
    schema: DOMAIN_STATUS_REQUEST,
    action: "withdraw",
    name: "@example/hello-skill",
    version: "1.0.0",
    archive_sha256: skillArchive,
    reason: "The script prints the wrong greeting",
    after_sequence: 1,
  }
  const unwithdraw = { ...withdraw, action: "unwithdraw", reason: null, after_sequence: 2 }
  const at = (sequence: number, state: State): VersionStatus => ({ archive_sha256: skillArchive, sequence, state })
  type ReqCase = {
    name: string
    note: string
    path: { action: string; name: string; version: string }
    request: { statement: Record<string, unknown>; public_key: string; signature: string }
    current: VersionStatus
    scope_keys: ScopeKey[]
    expect: StatementResult
  }
  const rq = (
    name: string,
    note: string,
    statement: Record<string, unknown>,
    currentStatus: VersionStatus,
    expect: StatementResult,
    o: { pathAction?: string; key?: TestKey; keys?: ScopeKey[]; domain?: string; flip?: boolean } = {},
  ): ReqCase => {
    const key = o.key ?? K1
    const sigHex = signedStatement(statement, key, o.domain ?? DOMAIN_STATUS_REQUEST).signature
    return {
      name,
      note,
      path: { action: o.pathAction ?? (statement.action as string), name: "@example/hello-skill", version: "1.0.0" },
      request: { statement, public_key: key.publicKeyHex, signature: o.flip ? flipFirstBit(sigHex) : sigHex },
      current: currentStatus,
      scope_keys: o.keys ?? [current(K1)],
      expect,
    }
  }
  const requestCases: ReqCase[] = [
    rq("withdraw", "", withdraw, at(1, "available"), { ok: true, result: "applied", next: { sequence: 2, state: "withdrawn" } }),
    rq("unwithdraw", "reason may be null when reversing a withdrawal", unwithdraw, at(2, "withdrawn"), { ok: true, result: "applied", next: { sequence: 3, state: "available" } }),
    rq("replayed_withdraw", "the same signed request after it applied", withdraw, at(2, "withdrawn"), { ok: false, code: "status_sequence_conflict" }),
    rq("replayed_after_reversal", "the state would allow a withdrawal again, but the sequence has moved on", withdraw, at(3, "available"), { ok: false, code: "status_sequence_conflict" }),
    rq("after_sequence_ahead", "", { ...withdraw, after_sequence: 5 }, at(1, "available"), { ok: false, code: "status_sequence_conflict" }),
    rq("withdraw_withdrawn", "", { ...withdraw, after_sequence: 2 }, at(2, "withdrawn"), { ok: false, code: "action_not_allowed" }),
    rq("unwithdraw_available", "", { ...unwithdraw, after_sequence: 1 }, at(1, "available"), { ok: false, code: "action_not_allowed" }),
    rq("withdraw_tombstoned", "", { ...withdraw, after_sequence: 4 }, at(4, "tombstoned"), { ok: false, code: "action_not_allowed" }),
    rq("withdraw_without_reason", "", { ...withdraw, reason: null }, at(1, "available"), { ok: false, code: "statement_invalid", field: "reason" }),
    rq("unknown_action", "", { ...withdraw, action: "delete" }, at(1, "available"), { ok: false, code: "statement_invalid", field: "action" }, { pathAction: "withdraw" }),
    rq("after_sequence_zero", "", { ...withdraw, after_sequence: 0 }, at(1, "available"), { ok: false, code: "statement_invalid", field: "after_sequence" }),
    rq("posted_to_other_path", "a withdraw statement sent to /unwithdraw", withdraw, at(1, "available"), { ok: false, code: "record_mismatch", field: "action" }, { pathAction: "unwithdraw" }),
    rq("bit_flip", "", withdraw, at(1, "available"), { ok: false, code: "signature_invalid" }, { flip: true }),
    rq("record_domain_tag", "signed under ck-record/v1", withdraw, at(1, "available"), { ok: false, code: "signature_invalid" }, { domain: DOMAIN_RECORD }),
    rq("key_outside_scope", "valid signature by a key not in @example's set", withdraw, at(1, "available"), { ok: false, code: "key_unknown" }, { key: K2 }),
    rq("revoked_key", "", withdraw, at(1, "available"), { ok: false, code: "key_revoked" }, {
      keys: [current(K1, { revocation: { reason: "key compromise", compromise_from: "2026-10-20T00:00:00Z" } })],
    }),
    rq("retired_key", "a rotated-out key can no longer act for the scope", withdraw, at(1, "available"), { ok: false, code: "key_not_valid" }, {
      keys: [current(K1, { valid_until: "2026-06-01T00:00:00Z" }), current(K3, { valid_from: "2026-06-01T00:00:00Z" })],
    }),
    rq("other_archive", "", { ...withdraw, archive_sha256: sha256Hex(FLOW_PACKAGE.archive) }, at(1, "available"), { ok: false, code: "record_mismatch", field: "archive_sha256" }),
  ]
  for (const k of requestCases) {
    const got = applyStatusRequest(k.path, { statement_text: jcs(k.request.statement), public_key: k.request.public_key, signature: k.request.signature }, k.current, k.scope_keys)
    expectSame(`status request case ${k.name}`, got, k.expect)
  }

  return {
    schema: "ck-catalog-v1/statements",
    signing: {
      signing_input: "ASCII schema string as the domain tag, one 0x00 byte, then the UTF-8 JCS bytes of the parsed statement",
      algorithm: "Ed25519 (RFC 8032, pure, deterministic)",
      key_windows: "half open: valid_from is inside, valid_until is not",
    },
    keys: {
      WARNING: "Every key below is TEST-ONLY with a published seed. Each role has its own keys: the root, the registry status keys, @cortexkit publish keys and third-party publisher keys.",
      root: keyOut(ROOT),
      registry_status: [keyOut(R1), keyOut(R2), keyOut(R3)],
      cortexkit_publish: [keyOut(C1), keyOut(C2)],
      publishers: [keyOut(K1), keyOut(K2), keyOut(K3)],
    },
    "ck-registry-key/v1": {
      fields: ["schema", "registry", "key_id", "public_key", "valid_from"],
      served_as: "GET /v1/registry/keys: {schema: 'ck-registry-keys/v1', registry, keys: [{statement, signature}]} in chain order from the first introduced key",
      rule: "each statement is signed by the newest key the client already holds for that registry, starting from the key pinned with the allowlist entry; a held key id or public key with other bytes is key_conflict; bytes already accepted are a duplicate",
      chain: [
        { ...statementOut(st2, DOMAIN_REGISTRY_KEY), signed_by: "test-registry-1", signature: signedStatement(st2, R1, DOMAIN_REGISTRY_KEY).signature },
        { ...statementOut(st3, DOMAIN_REGISTRY_KEY), signed_by: "test-registry-2", signature: signedStatement(st3, R2, DOMAIN_REGISTRY_KEY).signature },
      ],
      cases: registryKeyCases,
    },
    "ck-key-rotation/v1": {
      fields: ["schema", "scope", "old_key", "new_key"],
      served_as: "{statement, old_signature, new_signature}; both signatures over the same signing input",
      order: ["statement_invalid", "record_mismatch (scope)", "signature_invalid (old_signature, then new_signature)", "key_unknown / key_revoked / key_not_valid (old key must be current)", "key_reused (registry only)"],
      valid: { ...statementOut(rotation, DOMAIN_KEY_ROTATION), old_signature: signedStatement(rotation, K1, DOMAIN_KEY_ROTATION).signature, new_signature: signedStatement(rotation, K3, DOMAIN_KEY_ROTATION).signature },
      cases: rotationCases,
    },
    "ck-key-cert/v1": {
      fields: ["schema", "scope", "public_key", "valid_from", "valid_until"],
      served_as: "{statement, signature} beside each @cortexkit key in GET /v1/scopes/cortexkit; verified against the compiled-in root public key",
      valid: [
        { name: "bounded_cert", ...statementOut(cert1, DOMAIN_KEY_CERT), signature: signedStatement(cert1, ROOT, DOMAIN_KEY_CERT).signature },
        { name: "open_ended_cert", ...statementOut(cert2, DOMAIN_KEY_CERT), signature: signedStatement(cert2, ROOT, DOMAIN_KEY_CERT).signature },
      ],
      cases: certCases,
      window_cases: windowCases,
    },
    "ck-status-request/v1": {
      fields: ["schema", "action", "name", "version", "archive_sha256", "reason", "after_sequence"],
      served_as: "POST /v1/packages/{scope}/{name}/{version}/withdraw or /unwithdraw with body {statement, public_key, signature}",
      order: [
        "statement_invalid",
        "record_mismatch (action, name, version against the path)",
        "signature_invalid",
        "key_unknown / key_revoked / key_not_valid (a current key of the scope)",
        "record_mismatch (archive_sha256)",
        "status_sequence_conflict (after_sequence must equal the current sequence)",
        "action_not_allowed (withdraw only from available, unwithdraw only from withdrawn)",
      ],
      valid: [
        { ...statementOut(withdraw, DOMAIN_STATUS_REQUEST), public_key: K1.publicKeyHex, signature: signedStatement(withdraw, K1, DOMAIN_STATUS_REQUEST).signature },
        { ...statementOut(unwithdraw, DOMAIN_STATUS_REQUEST), public_key: K1.publicKeyHex, signature: signedStatement(unwithdraw, K1, DOMAIN_STATUS_REQUEST).signature },
      ],
      cases: requestCases,
    },
  }
}

// ───────────────────────────── archive strictness ─────────────────────────────

function archiveVectors() {
  const base = SKILL_PACKAGE.members.map((m) => tarEntry(m.path, m.content))
  const valid = writeTar(base)
  if (!valid.equals(SKILL_PACKAGE.archive)) throw new Error("archive: writer does not reproduce the skill archive")
  const blockSpan = (e: TarEntry) => 512 + Math.ceil(e.data.length / 512) * 512
  const offsetOf = (entries: TarEntry[], index: number) => entries.slice(0, index).reduce((n, e) => n + blockSpan(e), 0)
  const withHeader = (index: number, extra: Partial<HeaderSpec>) => base.map((e, i) => (i === index ? { header: { ...e.header, ...extra }, data: e.data } : e))
  const end = valid.length - 1024 // offset of the terminator in the valid archive
  const ckSize = base[0].data.length
  const base256Size = Buffer.alloc(12)
  base256Size[0] = 0x80
  base256Size.writeUIntBE(ckSize, 6, 6)
  const spacedSize = Buffer.from(` ${ckSize.toString(8).padStart(10, "0")}\0`, "latin1")
  const alteredName = Buffer.from(valid)
  alteredName[0] = "d".charCodeAt(0) // ck.json -> dk.json without recomputing the checksum
  const nonZeroBlock = Buffer.alloc(512)
  nonZeroBlock[100] = 0x01
  const pad = (count: number) => Array.from({ length: count }, (_, i) => tarEntry(`pad/${String(i + 1).padStart(4, "0")}`, ""))
  const appended = (...extra: TarEntry[]) => writeTar([...base, ...extra])
  const paxBody = "16 path=ck.json\n"
  if (paxBody.length !== 16) throw new Error("archive: pax record length")
  const longPath = { prefix: "p".repeat(155), name: "n".repeat(100) }

  type Expect = { ok: true; member_count: number } | { ok: false; code: "archive_invalid"; rule: ArchiveRule; offset?: number; member?: string; typeflag?: string }
  type Case = { name: string; note: string; archive: Buffer; build?: Record<string, unknown>; expect: Expect }
  const refuse = (rule: ArchiveRule, extra: { offset?: number; member?: string; typeflag?: string } = {}): Expect => ({ ok: false, code: "archive_invalid", rule, ...extra })
  const cases: Case[] = [
    { name: "valid_with_trailing_zero_padding", note: "zero bytes after the terminator are allowed (tar's 10 KiB record padding)", archive: Buffer.concat([valid, Buffer.alloc(2048)]), expect: { ok: true, member_count: 3 } },
    { name: "header_checksum_off_by_one", note: "the stored checksum is the true sum plus one", archive: writeTar(withHeader(0, { checksumDelta: 1 })), expect: refuse("header_checksum", { offset: 0 }) },
    { name: "header_altered_after_checksum", note: "one name byte changed; the checksum still describes the old header", archive: alteredName, expect: refuse("header_checksum", { offset: 0 }) },
    { name: "gnu_magic", note: "old GNU format: magic 'ustar ' and version ' \\0'", archive: writeTar(withHeader(0, { magic: "ustar ", version: " \0" })), expect: refuse("magic", { offset: 0 }) },
    { name: "wrong_version", note: "magic 'ustar\\0' with version '01'", archive: writeTar(withHeader(0, { version: "01" })), expect: refuse("magic", { offset: 0 }) },
    { name: "v7_no_magic", note: "pre-POSIX header: magic and version all zero", archive: writeTar(withHeader(0, { magic: "\0\0\0\0\0\0", version: "\0\0" })), expect: refuse("magic", { offset: 0 }) },
    { name: "base256_size", note: "size as GNU base-256 (first byte 0x80, value big-endian)", archive: writeTar(withHeader(0, { sizeField: base256Size })), expect: refuse("size_encoding", { offset: 0 }) },
    { name: "size_with_leading_space", note: "octal digits must start at the field's first byte", archive: writeTar(withHeader(0, { sizeField: spacedSize })), expect: refuse("size_encoding", { offset: 0 }) },
    { name: "terminator_missing", note: "archive ends right after the last member", archive: valid.subarray(0, end), expect: refuse("terminator_missing", { offset: end }) },
    { name: "terminator_one_block", note: "only one of the two zero blocks", archive: valid.subarray(0, end + 512), expect: refuse("terminator_missing", { offset: end }) },
    {
      name: "member_after_single_zero_block",
      note: "one zero block is not an end; a member after it is refused",
      archive: Buffer.concat([writeTar(base, Buffer.alloc(512)), writeTar([tarEntry("late.txt", "late\n")])]),
      expect: refuse("terminator_missing", { offset: end }),
    },
    { name: "non_zero_after_terminator", note: "offset is the first non-zero byte after the terminator", archive: Buffer.concat([valid, nonZeroBlock]), expect: refuse("trailing_data", { offset: valid.length + 100 }) },
    {
      name: "pax_extended_header",
      note: "typeflag x before ck.json",
      archive: writeTar([tarEntry("PaxHeaders/ck.json", paxBody, { typeflag: "x" }), ...base]),
      expect: refuse("member_type", { offset: 0, typeflag: "x" }),
    },
    {
      name: "gnu_long_name",
      note: "typeflag L (GNU long name)",
      archive: writeTar([tarEntry("././@LongLink", "ck.json\0", { typeflag: "L" }), ...base]),
      expect: refuse("member_type", { offset: 0, typeflag: "L" }),
    },
    { name: "symlink", note: "typeflag 2", archive: appended(tarEntry("evil", "", { typeflag: "2", linkname: "/etc/passwd" })), expect: refuse("member_type", { offset: end, typeflag: "2" }) },
    { name: "hard_link", note: "typeflag 1", archive: appended(tarEntry("again.json", "", { typeflag: "1", linkname: "ck.json" })), expect: refuse("member_type", { offset: end, typeflag: "1" }) },
    { name: "directory_entry", note: "typeflag 5", archive: appended(tarEntry("scripts/", "", { typeflag: "5" })), expect: refuse("member_type", { offset: end, typeflag: "5" }) },
    { name: "dot_dot_path", note: "", archive: appended(tarEntry("../evil", "x\n")), expect: refuse("path_invalid", { offset: end, member: "../evil" }) },
    { name: "absolute_path", note: "", archive: appended(tarEntry("/etc/passwd", "x\n")), expect: refuse("path_invalid", { offset: end, member: "/etc/passwd" }) },
    { name: "non_ascii_path", note: "one latin-1 byte 0xe9", archive: appended(tarEntry("\u00e9.md", "x\n")), expect: refuse("path_invalid", { offset: end, member: "\u00e9.md" }) },
    {
      name: "path_over_255_bytes",
      note: "prefix 155 + '/' + name 100 = 256 bytes: fits the header fields but not the 255-byte cap",
      archive: appended({ header: { ...longPath, size: 2 }, data: Buffer.from("x\n") }),
      expect: refuse("path_too_long", { offset: end, member: `${longPath.prefix}/${longPath.name}` }),
    },
    { name: "duplicate_member", note: "", archive: appended(tarEntry("SKILL.md", "again\n")), expect: refuse("path_duplicate", { offset: end, member: "SKILL.md" }) },
    { name: "case_collision", note: "equal under ASCII lower-casing", archive: appended(tarEntry("skill.md", "again\n")), expect: refuse("path_collision", { offset: end, member: "skill.md" }) },
    {
      name: "truncated_member",
      note: "the header claims 4096 bytes; 10 follow",
      archive: Buffer.concat([writeTar(base, Buffer.alloc(0)), tarHeader({ path: "big.txt", size: 4096 }), Buffer.from("0123456789")]),
      expect: refuse("truncated", { offset: end, member: "big.txt" }),
    },
    { name: "ck_json_missing", note: "", archive: writeTar(base.slice(1)), expect: refuse("ck_json_missing") },
    {
      name: "members_at_limit",
      note: "1,000 members in all",
      archive: writeTar([...base, ...pad(997)]),
      build: { base: "valid", append_empty_members: { path_format: "pad/NNNN, NNNN a 4-digit decimal counting from 0001", first: "pad/0001", last: "pad/0997" } },
      expect: { ok: true, member_count: 1000 },
    },
    {
      name: "too_many_members",
      note: "1,001 members: refused at the 1,001st header",
      archive: writeTar([...base, ...pad(998)]),
      build: { base: "valid", append_empty_members: { path_format: "pad/NNNN, NNNN a 4-digit decimal counting from 0001", first: "pad/0001", last: "pad/0998" } },
      expect: refuse("too_many_members", { offset: offsetOf([...base, ...pad(998)], 1000), member: "pad/0998" }),
    },
  ]
  for (const c of cases) {
    const r = readArchive(c.archive)
    const got = r.ok ? { ok: true, member_count: r.members.size } : r
    expectSame(`archive case ${c.name}`, got, c.expect)
  }
  const validRead = readArchive(valid)
  if (!validRead.ok) throw new Error("archive: valid archive refused")

  const pathCases: [string, PathRule | null, string][] = [
    ["SKILL.md", null, ""],
    ["scripts/hello.sh", null, ""],
    ["a".repeat(100), null, "100 bytes: fits the name field alone"],
    ["a".repeat(101), "path_not_ustar", "a single component over 100 bytes can never be split"],
    [`d/${"n".repeat(101)}`, "path_not_ustar", "the last component is over 100 bytes"],
    [`${"p".repeat(155)}/${"n".repeat(99)}`, null, "255 bytes: prefix 155, name 99"],
    [`${"p".repeat(155)}/${"n".repeat(100)}`, "path_too_long", "256 bytes: would fit the fields, but over the 255-byte cap"],
    [`${"p".repeat(156)}/${"n".repeat(50)}`, "path_not_ustar", "the only split leaves a 156-byte prefix"],
    [`${"p".repeat(100)}/${"q".repeat(53)}/${"n".repeat(100)}`, null, "255 bytes split at the second '/': prefix 154, name 100"],
    ["../evil", "path_invalid", "'..' component"],
    ["a/./b", "path_invalid", "'.' component"],
    ["a//b", "path_invalid", "empty component"],
    ["/etc/passwd", "path_invalid", "leading '/'"],
    ["dir/", "path_invalid", "trailing '/'"],
    ["a\\b", "path_invalid", "backslash"],
    ["a b", "path_invalid", "space"],
    ["\u00e9.md", "path_invalid", "non-ASCII"],
    ["", "path_invalid", "empty"],
  ]
  const pathOut = pathCases.map(([path, rule, note]) => {
    const got = checkPath(path)
    expectSame(`path ${JSON.stringify(path)}`, got, rule === null ? { ok: true } : { ok: false, rule })
    return { path, path_bytes: Buffer.byteLength(path), note, expect: got, ustar_split: got.ok ? ustarSplit(path) : null }
  })

  return {
    schema: "ck-catalog-v1/archive",
    rules: {
      refusal: "{code: 'archive_invalid', rule, offset?, member?, typeflag?}; offset is the byte offset of the header block concerned (or of the terminator, or of the first non-zero trailing byte); member is the path once it has been read",
      max_members: MAX_MEMBERS,
      max_path_bytes: PATH_MAX,
      reader_order: [
        "fewer than 512 bytes left: terminator_missing",
        "a zero block: the next block must also be zero (else terminator_missing), then every later byte must be zero (else trailing_data); done",
        "header_checksum: the checksum field is octal digits from its first byte, then only NUL or space; it must equal the sum of the 512 header bytes with the checksum field read as 8 spaces",
        "magic: bytes 257-262 'ustar\\0' and bytes 263-264 '00'",
        "member_type: typeflag '0' or NUL only",
        "size_encoding: the size field is octal digits from its first byte, then only NUL or space (no base-256)",
        "path = prefix + '/' + name when prefix is non-empty, else name (each field up to its first NUL); path_invalid, then path_too_long",
        "too_many_members: more than 1,000 members",
        "path_duplicate, then path_collision (equal under ASCII lower-casing)",
        "truncated: the member's data runs past the end of the archive",
        "after the terminator: ck_json_missing",
      ],
      path_rules: "also applied to every files[].path in ck.json (manifest_invalid) and by the packer: components of [A-Za-z0-9._-], no empty, '.' or '..' component (path_invalid); at most 255 bytes (path_too_long); storable as a ustar name of at most 100 bytes, optionally after a prefix of at most 155 bytes ending at a '/' (path_not_ustar)",
    },
    writer: {
      header:
        "512 bytes, zero-filled, then: name (offset 0, 100 bytes) and prefix (345, 155) from the ustar split; mode (100) '0000644\\0'; uid (108) and gid (116) '0000000\\0'; size (124) 11 octal digits then NUL; mtime (136) '00000000000\\0'; typeflag (156) '0'; magic (257) 'ustar\\0'; version (263) '00'; checksum (148) 6 octal digits, NUL, space, computed with the checksum field read as 8 spaces",
      split: "a path of at most 100 bytes goes in name; a longer one splits at the first '/' that leaves at most 100 bytes after it and at most 155 before it",
      data: "member bytes, then zero padding to a 512-byte boundary",
      end: "two 512-byte zero blocks and nothing after",
    },
    valid: {
      note: "the skill package archive of record.json",
      archive_base64: valid.toString("base64"),
      archive_sha256: sha256Hex(valid),
      archive_bytes: valid.length,
      members: [...validRead.members].map(([path, bytes]) => ({ path, sha256: sha256Hex(bytes), bytes: bytes.length })),
    },
    cases: cases.map((c) => ({
      name: c.name,
      note: c.note,
      ...(c.build ? { build: c.build } : { archive_base64: c.archive.toString("base64") }),
      archive_sha256: sha256Hex(c.archive),
      archive_bytes: c.archive.length,
      expect: c.expect,
    })),
    path_cases: pathOut,
  }
}

// ───────────────────────────── starter bundle ─────────────────────────────

function starterBundleVectors() {
  const created = "2026-11-01T00:00:00Z"
  const persona = makePackage(
    "starter-persona",
    {
      schema: "ck-package/v1",
      name: "@cortexkit/starter-persona",
      version: "1.0.0",
      kind: "persona",
      summary: "A minimal first-party starter persona",
      license: "MIT",
      source: "https://example.invalid/starter-persona",
      requires: { capabilities: [], packages: {} },
      body: {
        prompt: "PERSONA.md",
        implements: [],
        delegates_to: {},
        capabilities: { required: [], optional: [] },
        skills: [],
        flows: [],
      },
    },
    [{ path: "PERSONA.md", content: "# Starter persona\n\nA minimal test-vector persona.\n" }],
  )
  const skill = makePackage(
    "starter-skill",
    {
      schema: "ck-package/v1",
      name: "@cortexkit/starter-skill",
      version: "1.0.0",
      kind: "skill",
      summary: "A minimal first-party starter skill",
      license: "MIT",
      source: "https://example.invalid/starter-skill",
      requires: { capabilities: [], packages: {} },
      body: { entry: "SKILL.md", audience: "user", scripts: [] },
    },
    [{ path: "SKILL.md", content: "# Starter skill\n\nA minimal test-vector skill.\n" }],
  )
  const publisher = TEST_CORTEXKIT_KEY_1
  const root = TEST_ROOT_KEY
  const registry = TEST_REGISTRY_KEY_1
  const packages = [persona, skill]
  const certificate = {
    schema: DOMAIN_KEY_CERT,
    scope: "cortexkit",
    public_key: publisher.publicKeyHex,
    valid_from: "2026-10-01T00:00:00Z",
    valid_until: "2027-04-01T00:00:00Z",
  }
  const certificateSignature = signCanonical(certificate, root, DOMAIN_KEY_CERT)
  const signedCertificate = { statement: certificate, signature: certificateSignature.signature }
  const certificateText = jcs(signedCertificate)

  const packageVectors = packages.map((pkg) => {
    const record = recordFor(pkg, publisher)
    const recordSigned = signCanonical(record, publisher, DOMAIN_RECORD)
    const status = {
      ...statusRecord(pkg, { sequence: 1, state: "available", reason: null, published_at: created, changed_at: created }),
      publisher_key: publisher.fingerprint,
    }
    const statusSigned = signCanonical(status, registry, DOMAIN_STATUS)
    const recordEnvelope = { record, signature: recordSigned.signature }
    const statusEnvelope = { record: status, signature: statusSigned.signature }
    const recordFile = { record: recordEnvelope, status: statusEnvelope }
    const packageName = pkg.manifest.name!.toString().split("/")[1]
    const recordPath = `records/cortexkit/${packageName}/${pkg.manifest.version}.json`
    const archivePath = `archives/${sha256Hex(pkg.archive)}.tar`
    return {
      pkg,
      recordPath,
      archivePath,
      record,
      recordCanonical: recordSigned.canonical,
      recordSignature: recordSigned.signature,
      recordSigningInputHex: signingInput(DOMAIN_RECORD, recordSigned.canonical).toString("hex"),
      status,
      statusCanonical: statusSigned.canonical,
      statusSignature: statusSigned.signature,
      statusSigningInputHex: signingInput(DOMAIN_STATUS, statusSigned.canonical).toString("hex"),
      recordFile,
      recordFileText: jcs(recordFile),
    }
  })

  const bundleDocument = {
    schema: DOMAIN_STARTER_BUNDLE,
    created,
    records: packageVectors.map((v) => ({
      name: v.pkg.manifest.name,
      version: v.pkg.manifest.version,
      archive_sha256: sha256Hex(v.pkg.archive),
      record_member_sha256: sha256Hex(v.recordFileText),
      record: v.recordPath,
      archive: v.archivePath,
    })),
  }
  const bundleCanonical = jcs(bundleDocument)
  const bundleSigningInput = signingInput(DOMAIN_STARTER_BUNDLE, bundleCanonical)
  const bundleSignature = signHex(publisher, bundleSigningInput)
  const entries: TarEntry[] = [
    tarEntry("bundle.json", bundleCanonical),
    tarEntry("bundle.cert.json", certificateText),
    ...packageVectors.map((v) => tarEntry(v.recordPath, v.recordFileText)),
    ...packageVectors.map((v) => tarEntry(v.archivePath, v.pkg.archive)),
    tarEntry("bundle.sig", bundleSignature),
  ]
  const archiveFor = (items: TarEntry[]) => writeTar(items)
  const validArchive = archiveFor(entries)
  const packageByName = new Map(packageVectors.map((v) => [v.pkg.manifest.name as string, v.pkg]))
  const registryKeys = { [TEST_REGISTRY_KEY_ID]: registry.publicKeyHex }

  type BundleArchiveRule = ArchiveRule | "member_unlisted"
  type BundleResult = { ok: true } | { ok: false; code?: string; field?: string; rule?: BundleArchiveRule; offset?: number; member?: string; typeflag?: string }
  const refused = (code: string, extra: { field?: string; rule?: BundleArchiveRule; offset?: number; member?: string; typeflag?: string } = {}): BundleResult => ({ ok: false, code, ...extra })
  const offsetOfMember = (bytes: Buffer, target: string): number | null => {
    let at = 0
    while (at + 512 <= bytes.length) {
      const header = bytes.subarray(at, at + 512)
      if (header.every((byte) => byte === 0)) return null
      const size = octalField(header.subarray(124, 136))
      if (size === null) return null
      const name = textField(header, 0, 100)
      const prefix = textField(header, 345, 155)
      if ((prefix ? `${prefix}/${name}` : name) === target) return at
      at += 512 + Math.ceil(size / 512) * 512
    }
    return null
  }
  const verifyBundle = (bytes: Buffer): BundleResult => {
    const read = readArchive(bytes, null)
    if (!read.ok) return read
    const members = read.members
    if (!members.has("bundle.json")) return { ok: false }
    let parsed: unknown
    try {
      parsed = parseStrictJson(members.get("bundle.json")!.toString("utf8"))
    } catch {
      return { ok: false }
    }
    if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed)) return { ok: false }
    const bundle = parsed as { schema?: unknown; created?: unknown; records?: unknown }
    if (bundle.schema !== DOMAIN_STARTER_BUNDLE || typeof bundle.created !== "string" || !Array.isArray(bundle.records)) return { ok: false }

    const certBytes = members.get("bundle.cert.json")
    if (!certBytes) return refused("key_unknown")
    let certEnvelope: unknown
    try {
      certEnvelope = parseStrictJson(certBytes.toString("utf8"))
    } catch {
      return { ok: false }
    }
    if (typeof certEnvelope !== "object" || certEnvelope === null || Array.isArray(certEnvelope)) return { ok: false }
    const cert = certEnvelope as { statement?: Record<string, unknown>; signature?: string }
    if (!cert.statement || typeof cert.signature !== "string") return { ok: false }
    const certResult = verifyKeyCert(jcs(cert.statement), cert.signature, root.publicKeyHex)
    if (!certResult.ok) return refused(certResult.code, "field" in certResult ? { field: certResult.field } : {})
    if (!windowCovers(cert.statement as { valid_from: string; valid_until: string | null }, bundle.created)) return refused("key_not_valid")

    const signature = members.get("bundle.sig")?.toString("ascii")
    if (!signature || !verifyHex(cert.statement.public_key as string, signingInput(DOMAIN_STARTER_BUNDLE, jcs(bundle)), signature)) {
      return refused("signature_invalid")
    }

    const expectedMembers = new Set(["bundle.json", "bundle.cert.json", "bundle.sig"])
    for (const item of bundle.records) {
      if (typeof item !== "object" || item === null || Array.isArray(item)) return { ok: false }
      const row = item as { name?: unknown; version?: unknown; archive_sha256?: unknown; record_member_sha256?: unknown; record?: unknown; archive?: unknown }
      if (typeof row.name !== "string" || typeof row.version !== "string" || typeof row.archive_sha256 !== "string" || typeof row.record_member_sha256 !== "string" || typeof row.record !== "string" || typeof row.archive !== "string") return { ok: false }
      expectedMembers.add(row.record)
      expectedMembers.add(row.archive)
      const nameParts = row.name.slice(1).split("/")
      const expectedRecordPath = `records/${nameParts[0]}/${nameParts[1]}/${row.version}.json`
      if (row.record !== expectedRecordPath) return { ok: false }
      const recordBytes = members.get(row.record)
      if (!recordBytes) return refused("record_invalid", { field: "record" })
      if (sha256Hex(recordBytes) !== row.record_member_sha256) {
        return refused("record_mismatch", { field: "record_member_sha256", member: row.record })
      }
      const componentArchive = members.get(row.archive)
      if (!componentArchive || sha256Hex(componentArchive) !== row.archive_sha256 || row.archive !== `archives/${row.archive_sha256}.tar`) {
        return refused("record_mismatch", { field: "archive_sha256" })
      }
      const sourcePackage = packageByName.get(row.name)
      if (!sourcePackage || sourcePackage.manifest.version !== row.version) return { ok: false }
      const recordText = recordBytes.toString("utf8")
      let recordFile: unknown
      try {
        recordFile = parseStrictJson(recordText)
      } catch {
        return refused("record_invalid", { field: "json" })
      }
      if (typeof recordFile !== "object" || recordFile === null || Array.isArray(recordFile)) return refused("record_invalid", { field: "json" })
      const document = recordFile as { record?: { record?: Record<string, unknown>; signature?: string }; status?: { record?: Record<string, unknown>; signature?: string } }
      if (!document.record?.record || typeof document.record.signature !== "string") return refused("record_invalid", { field: "record" })
      if (!document.status?.record || typeof document.status.signature !== "string") return refused("status_invalid", { field: "status" })

      const recordResult = verifyRecord(
        jcs(document.record.record),
        document.record.signature,
        { ...sourcePackage, archive: componentArchive },
        [keyFingerprint(cert.statement.public_key as string)],
      )
      if (!recordResult.ok) return refused(recordResult.code, "field" in recordResult ? { field: recordResult.field } : {})
      const statusResult = applyStatus(
        null,
        jcs(document.status.record),
        document.status.signature,
        registryKeys,
        { name: row.name, version: row.version, archive_sha256: row.archive_sha256 },
      )
      if (!statusResult.ok) return refused(statusResult.code, "field" in statusResult ? { field: statusResult.field } : {})
      if (document.status.record.publisher_key !== keyFingerprint(document.record.record.public_key as string)) return refused("record_mismatch", { field: "publisher_key" })
    }
    for (const path of members.keys()) {
      if (expectedMembers.has(path)) continue
      const offset = offsetOfMember(bytes, path)
      if (offset === null) return { ok: false }
      return refused("archive_invalid", { rule: "member_unlisted", offset, member: path })
    }
    return { ok: true }
  }

  type BundleCase = { name: string; note: string; archive: Buffer; expect: BundleResult }
  const replaceEntry = (path: string, data: string | Buffer, extra: Partial<HeaderSpec> = {}) =>
    entries.map((entry) => (entry.header.path === path ? tarEntry(path, data, extra) : entry))
  const corruptComponentArchive = Buffer.from(packageVectors[0].pkg.archive)
  corruptComponentArchive[512 + 1] ^= 0x01
  const firstRecord = packageVectors[0]
  const changedStatusRecord = {
    ...firstRecord.recordFile,
    status: {
      ...firstRecord.recordFile.status,
      record: {
        ...firstRecord.recordFile.status.record,
        sequence: firstRecord.recordFile.status.record.sequence + 1,
      },
    },
  }
  const missingStatus = { ...firstRecord.recordFile } as Record<string, unknown>
  delete missingStatus.status
  const missingStatusText = jcs(missingStatus)
  const missingStatusBundle = {
    ...bundleDocument,
    records: bundleDocument.records.map((row) => row.record === firstRecord.recordPath
      ? { ...row, record_member_sha256: sha256Hex(missingStatusText) }
      : row),
  }
  const missingStatusBundleCanonical = jcs(missingStatusBundle)
  const missingStatusBundleSignature = signHex(publisher, signingInput(DOMAIN_STARTER_BUNDLE, missingStatusBundleCanonical))
  const missingStatusEntries = entries.map((entry) => {
    if (entry.header.path === firstRecord.recordPath) return tarEntry(firstRecord.recordPath, missingStatusText)
    if (entry.header.path === "bundle.json") return tarEntry("bundle.json", missingStatusBundleCanonical)
    if (entry.header.path === "bundle.sig") return tarEntry("bundle.sig", missingStatusBundleSignature)
    return entry
  })
  const unrootedCert = {
    schema: DOMAIN_KEY_CERT,
    scope: "cortexkit",
    public_key: TEST_CORTEXKIT_KEY_2.publicKeyHex,
    valid_from: "2026-10-01T00:00:00Z",
    valid_until: null,
  }
  const unrootedCertSignature = signCanonical(unrootedCert, publisher, DOMAIN_KEY_CERT).signature
  const unrootedBundleSignature = signHex(TEST_CORTEXKIT_KEY_2, bundleSigningInput)
  const firstHeader = entries[0]
  const pathEscapeEntry = tarEntry("../outside.txt", "outside\n")
  const cases: BundleCase[] = [
    {
      name: "bad_bundle_signature",
      note: "the first signature byte is flipped; bundle.json and the root-certified signer are unchanged",
      archive: archiveFor(replaceEntry("bundle.sig", flipFirstBit(bundleSignature))),
      expect: refused("signature_invalid"),
    },
    {
      name: "bundle_key_not_root_certified",
      note: "the bundle signer is publisher key 2, but its certificate was signed by publish key 1 instead of the root",
      archive: archiveFor(replaceEntry("bundle.cert.json", jcs({ statement: unrootedCert, signature: unrootedCertSignature })).map((entry) => (entry.header.path === "bundle.sig" ? tarEntry("bundle.sig", unrootedBundleSignature) : entry))),
      expect: refused("signature_invalid"),
    },
    {
      name: "archive_sha256_mismatch",
      note: "one byte of the first component archive changes while bundle.json retains its original digest",
      archive: archiveFor(replaceEntry(firstRecord.archivePath, corruptComponentArchive)),
      expect: refused("record_mismatch", { field: "archive_sha256" }),
    },
    {
      name: "record_member_bytes_changed",
      note: "the first record member's status sequence changes while bundle.json and bundle.sig remain unchanged",
      archive: archiveFor(replaceEntry(firstRecord.recordPath, jcs(changedStatusRecord))),
      expect: refused("record_mismatch", { field: "record_member_sha256", member: firstRecord.recordPath }),
    },
    {
      name: "record_missing_status",
      note: "the first signed record file omits its latest status envelope",
      archive: archiveFor(missingStatusEntries),
      expect: refused("status_invalid", { field: "status" }),
    },
    {
      name: "non_ustar_entry",
      note: "bundle.json has a non-POSIX ustar magic while its header checksum remains valid",
      archive: archiveFor(replaceEntry("bundle.json", firstHeader.data, { magic: "ustar " })),
      expect: refused("archive_invalid", { rule: "magic", offset: 0 }),
    },
    {
      name: "path_escaping_entry",
      note: "a regular-file member uses a parent-directory path",
      archive: archiveFor([...entries, pathEscapeEntry]),
      expect: refused("archive_invalid", { rule: "path_invalid", offset: validArchive.length - 1024, member: "../outside.txt" }),
    },
    {
      name: "extra_unlisted_file",
      note: "a regular file is added without a reference from bundle.json",
      archive: archiveFor([...entries, tarEntry("unlisted.txt", "not listed\n")]),
      expect: { ok: false, code: "archive_invalid", rule: "member_unlisted", offset: validArchive.length - 1024, member: "unlisted.txt" },
    },
  ]
  for (const c of cases) expectSame(`starter bundle case ${c.name}`, verifyBundle(c.archive), c.expect)
  expectSame("starter bundle valid", verifyBundle(validArchive), { ok: true })

  const validRead = readArchive(validArchive, null)
  if (!validRead.ok || !validRead.members.has("bundle.json")) throw new Error("starter bundle: valid archive refused")

  const invalidPersonaManifest = {
    ...persona.manifest,
    body: { ...(persona.manifest.body as Record<string, unknown>), memory_scope: "project" },
  }
  const personaBodyFields = new Set([
    "prompt",
    "extends",
    "implements",
    "delegates_to",
    "capabilities",
    "routing",
    "activation",
    "skills",
    "flows",
    "memory_profile",
  ])
  const validateClosedPersonaBody = (body: unknown) => {
    if (typeof body !== "object" || body === null || Array.isArray(body)) return refused("manifest_invalid")
    return Object.keys(body).some((field) => !personaBodyFields.has(field)) ? refused("manifest_invalid") : { ok: true as const }
  }
  expectSame("valid starter persona body", validateClosedPersonaBody(persona.manifest.body), { ok: true })
  const invalidPersonaManifestResult = validateClosedPersonaBody(invalidPersonaManifest.body)
  expectSame("persona body carrying removed memory_scope", invalidPersonaManifestResult, refused("manifest_invalid"))
  const invalidPersonaManifestText = `${JSON.stringify(invalidPersonaManifest, null, 2)}\n`

  return {
    schema: "ck-catalog-v1/starter-bundle",
    domain: DOMAIN_STARTER_BUNDLE,
    format: {
      bundle_json: "JCS {schema, created, records: [{name, version, archive_sha256, record_member_sha256, record, archive}]} where record_member_sha256 hashes the exact record member bytes and record/archive are member paths",
      record_member: "JCS {record: {record, signature}, status: {record, signature}}",
      certificate_member: "bundle.cert.json is JCS {statement, signature} for ck-key-cert/v1, signed by the TEST-ONLY root",
      signature_member: "bundle.sig is 128 lowercase hexadecimal ASCII bytes",
      archive: "one strict ustar archive; component archives remain their own raw strict ustar bytes",
    },
    signing: {
      algorithm: "Ed25519 (RFC 8032, pure, deterministic)",
      signing_input: "ASCII(ck-starter-bundle/v1) || 0x00 || UTF-8(JCS(bundle.json))",
      signing_input_hex: bundleSigningInput.toString("hex"),
      canonical_bundle_json: bundleCanonical,
      signature: bundleSignature,
    },
    keys: {
      WARNING: "TEST-ONLY keys derived from published fixed seed text; never trust them outside these vectors.",
      root: keyOut(root),
      publish: keyOut(publisher),
      registry_status: keyOut(registry),
    },
    valid: {
      created,
      bundle_json: bundleDocument,
      bundle_json_jcs: bundleCanonical,
      bundle_signature: bundleSignature,
      bundle_signing_input_hex: bundleSigningInput.toString("hex"),
      bundle_archive_base64: validArchive.toString("base64"),
      bundle_archive_sha256: sha256Hex(validArchive),
      bundle_archive_bytes: validArchive.length,
      members: [...validRead.members].map(([path, bytes]) => ({ path, sha256: sha256Hex(bytes), bytes: bytes.length })),
      certificate: {
        statement: certificate,
        canonical: certificateSignature.canonical,
        signing_input_hex: signingInput(DOMAIN_KEY_CERT, certificateSignature.canonical).toString("hex"),
        signature: certificateSignature.signature,
      },
      packages: packageVectors.map((v) => ({
        name: v.pkg.manifest.name,
        version: v.pkg.manifest.version,
        kind: v.pkg.manifest.kind,
        record_path: v.recordPath,
        record_file: v.recordFile,
        archive_path: v.archivePath,
        archive_base64: v.pkg.archive.toString("base64"),
        archive_sha256: sha256Hex(v.pkg.archive),
        archive_bytes: v.pkg.archive.length,
        record: {
          value: v.record,
          canonical: v.recordCanonical,
          signing_input_hex: v.recordSigningInputHex,
          signature: v.recordSignature,
        },
        status: {
          value: v.status,
          canonical: v.statusCanonical,
          signing_input_hex: v.statusSigningInputHex,
          signature: v.statusSignature,
        },
      })),
    },
    cases: cases.map((c) => ({
      name: c.name,
      note: c.note,
      bundle_archive_base64: c.archive.toString("base64"),
      bundle_archive_sha256: sha256Hex(c.archive),
      bundle_archive_bytes: c.archive.length,
      expect: c.expect,
    })),
    persona_manifest_cases: [
      {
        name: "persona_body_memory_scope",
        note: "Persona bodies are closed, so memory authorization is not declared by the package.",
        manifest_text: invalidPersonaManifestText,
        expect: invalidPersonaManifestResult,
      },
    ],
  }
}

// ───────────────────────────── semver dialect ─────────────────────────────

export interface Version {
  major: number
  minor: number
  patch: number
  pre: (string | number)[]
}

const NUM = "(0|[1-9]\\d*)"
const PRE_ID = "(?:0|[1-9]\\d*|\\d*[a-zA-Z-][0-9a-zA-Z-]*)"
const PRE = `${PRE_ID}(?:\\.${PRE_ID})*`
const VERSION_RE = new RegExp(`^${NUM}\\.${NUM}\\.${NUM}(?:-(${PRE}))?$`)
const PARTIAL_RE = new RegExp(`^${NUM}(?:\\.${NUM}(?:\\.${NUM}(?:-(${PRE}))?)?)?$`)

function numbers(parts: (string | undefined)[]): number[] | null {
  const out = parts.filter((p): p is string => p !== undefined).map(Number)
  return out.every(Number.isSafeInteger) ? out : null
}

function preIds(pre: string | undefined): (string | number)[] {
  return pre === undefined ? [] : pre.split(".").map((id) => (/^\d+$/.test(id) ? Number(id) : id))
}

// Package versions are SemVer 2.0.0 without build metadata.
export function parseVersion(text: string): Version | null {
  const m = VERSION_RE.exec(text)
  if (!m) return null
  const n = numbers([m[1], m[2], m[3]])
  if (!n) return null
  return { major: n[0], minor: n[1], patch: n[2], pre: preIds(m[4]) }
}

export function compareVersions(a: Version, b: Version): number {
  for (const key of ["major", "minor", "patch"] as const) if (a[key] !== b[key]) return a[key] < b[key] ? -1 : 1
  if (!a.pre.length || !b.pre.length) return b.pre.length - a.pre.length
  for (let i = 0; i < Math.max(a.pre.length, b.pre.length); i++) {
    const x = a.pre[i]
    const y = b.pre[i]
    if (x === undefined) return -1
    if (y === undefined) return 1
    if (x === y) continue
    if (typeof x === "number" && typeof y === "number") return x < y ? -1 : 1
    if (typeof x === "number") return -1
    if (typeof y === "number") return 1
    return x < y ? -1 : 1
  }
  return 0
}

const v = (major: number, minor: number, patch: number): Version => ({ major, minor, patch, pre: [] })

// One comparator as a half-open or closed interval over release versions, or
// an exact version (the only form that may name a prerelease).
interface Bound {
  lo: Version | null
  loInclusive: boolean
  hi: Version | null
  hiInclusive: boolean
  exact: Version | null
}

function comparator(text: string): Bound | null {
  if (text === "*") return { lo: null, loInclusive: true, hi: null, hiInclusive: false, exact: null }
  const m = /^(\^|~|>=|<=|>|<|=)?(.+)$/.exec(text)
  if (!m) return null
  const op = m[1] ?? ""
  const p = PARTIAL_RE.exec(m[2])
  if (!p) return null
  const n = numbers([p[1], p[2], p[3]])
  if (!n) return null
  const k = n.length
  const pre = p[4]
  if (pre !== undefined && op !== "" && op !== "=") return null // prereleases only by exact name
  const lo = v(n[0], n[1] ?? 0, n[2] ?? 0)
  const next = k === 1 ? v(n[0] + 1, 0, 0) : v(n[0], n[1] + 1, 0) // first version above a partial
  const range = (lower: Version | null, loInclusive: boolean, upper: Version | null, hiInclusive: boolean): Bound => ({ lo: lower, loInclusive, hi: upper, hiInclusive, exact: null })
  switch (op) {
    case "":
    case "=":
      return k === 3 ? { lo: null, loInclusive: true, hi: null, hiInclusive: false, exact: { ...lo, pre: preIds(pre) } } : range(lo, true, next, false)
    case "^":
      if (n[0] > 0 || k === 1) return range(lo, true, v(n[0] + 1, 0, 0), false)
      if (n[1] > 0 || k === 2) return range(lo, true, v(0, n[1] + 1, 0), false)
      return range(lo, true, v(0, 0, n[2] + 1), false)
    case "~":
      return k === 1 ? range(lo, true, v(n[0] + 1, 0, 0), false) : range(lo, true, v(n[0], n[1] + 1, 0), false)
    case ">=":
      return range(lo, true, null, false)
    case ">":
      return k === 3 ? range(lo, false, null, false) : range(next, true, null, false)
    case "<":
      return range(null, true, lo, false)
    case "<=":
      return k === 3 ? range(null, true, lo, true) : range(null, true, next, false)
  }
  return null
}

export type Range = Bound[][]

// range := set ("||" set)*, set := comparator (whitespace comparator)*
export function parseRange(text: string): Range | null {
  if (text.trim() === "") return null
  const sets: Bound[][] = []
  for (const set of text.split("||")) {
    const parts = set.trim().split(/\s+/)
    if (parts.length === 1 && parts[0] === "") return null
    if (parts.length > 1 && parts.includes("*")) return null
    const bounds = parts.map(comparator)
    if (bounds.some((b) => b === null)) return null
    // A prerelease must be the only comparator in its ||-set, even when other bounds are redundant.
    if (bounds.length > 1 && bounds.some((b) => b?.exact?.pre.length)) return null
    sets.push(bounds as Bound[])
  }
  return sets
}

function inBound(version: Version, b: Bound): boolean {
  if (b.exact) return compareVersions(version, b.exact) === 0
  if (b.lo) {
    const c = compareVersions(version, b.lo)
    if (c < 0 || (c === 0 && !b.loInclusive)) return false
  }
  if (b.hi) {
    const c = compareVersions(version, b.hi)
    if (c > 0 || (c === 0 && !b.hiInclusive)) return false
  }
  return true
}

export function satisfies(version: Version, range: Range): boolean {
  if (version.pre.length) return range.some((set) => set.length === 1 && set[0].exact !== null && compareVersions(version, set[0].exact) === 0)
  return range.some((set) => set.every((b) => inBound(version, b)))
}

// ───────────────────────────── resolver ─────────────────────────────

// Capability areas and verbs start with a lowercase letter and contain no
// hyphens; @N versions are distinct capability tokens.
export const CAPABILITY_TOKEN = /^cap\/[a-z][a-z0-9]*(\.[a-z][a-z0-9]*)?@[1-9][0-9]*$/
const CAPABILITY_NAME_MAX = 64
const SUBC_CAPABILITY_NAME = /^[a-z][a-z0-9]*(-[a-z0-9]+)*\/v[1-9][0-9]*$/

function capabilityTokenToManifest(token: string): string | null {
  if (!CAPABILITY_TOKEN.test(token)) return null
  const [tokenBody, version] = token.slice(4).split("@")
  const [area, verb] = tokenBody.split(".")
  const name = `cap-${area}${verb ? `-${verb}` : ""}`
  if (name.length > CAPABILITY_NAME_MAX) return null
  return `${name}/v${version}`
}

function capabilityManifestToToken(capability: string): string | null {
  const match = /^cap-([a-z][a-z0-9]*)(?:-([a-z][a-z0-9]*))?\/v([1-9][0-9]*)$/.exec(capability)
  if (!match) return null
  const [, area, verb, version] = match
  const name = capability.slice(0, capability.indexOf("/v"))
  if (name.length > CAPABILITY_NAME_MAX) return null
  return `cap/${area}${verb ? `.${verb}` : ""}@${version}`
}

type Kind = "persona" | "skill" | "flow" | "memory-profile"
type RefField = "extends" | "skills" | "flows" | "memory_profile" | "delegates_to"
const REF_KIND: Record<RefField, Kind> = { extends: "persona", skills: "skill", flows: "flow", memory_profile: "memory-profile", delegates_to: "persona" }

export interface RegistryVersion {
  version: string
  kind: Kind
  status: State
  requires: { capabilities: string[]; packages: Record<string, string> }
  refs?: { field: RefField; package: string }[]
}

export type Refusal = { code: string; [detail: string]: unknown }
export type Resolution = { ok: true; picks: Record<string, string>; moves?: Record<string, { from: string | null; to: string | null; direction: string }> } | { ok: false; refusal: Refusal }

const byName = (a: string, b: string) => (a < b ? -1 : a > b ? 1 : 0)

// Deterministic, non-backtracking resolution. Ranges accumulate: once a picked
// version has asked for a range, that range stays for the rest of the
// resolution even if the asker's own pick later moves. The candidate set for a
// package therefore only shrinks, picks only move down, and the loop ends.
type RootInput = { package: string; range: string } | { packages: Record<string, string> }

export function resolve(
  registry: Record<string, RegistryVersion[]>,
  root: RootInput,
  localCapabilities: string[],
  currentPins?: Record<string, string>,
): Resolution {
  const ROOT = "(root)"
  const rootPackages = "packages" in root ? root.packages : { [root.package]: root.range }
  const rootNames = Object.keys(rootPackages).sort(byName)
  const ranges = new Map<string, Map<string, string>>()
  for (const pkg of rootNames) ranges.set(pkg, new Map([[ROOT, rootPackages[pkg]]]))
  const eligible = (pkg: string) =>
    registry[pkg].filter((rv) => rv.status === "available" && rv.requires.capabilities.every((t) => localCapabilities.includes(t)))
  let picks = new Map<string, RegistryVersion>()
  for (;;) {
    // One pass: walk breadth-first from the synthetic root through this pass's
    // picks, dependencies in name order. Each package is picked against every
    // range recorded so far, and its own ranges are recorded before its
    // dependencies are visited.
    picks = new Map()
    let added = false
    const queue = [...rootNames]
    while (queue.length) {
      const pkg = queue.shift()!
      if (picks.has(pkg)) continue
      if (!registry[pkg]) return { ok: false, refusal: { code: "package_not_found", package: pkg } }
      const asked = [...(ranges.get(pkg) ?? new Map()).entries()].sort(([a], [b]) => byName(a, b))
      const parsed: Range[] = []
      for (const [by, text] of asked) {
        const r = parseRange(text)
        if (!r) return { ok: false, refusal: { code: "range_invalid", package: pkg, range: text, requested_by: by } }
        parsed.push(r)
      }
      const usable = eligible(pkg)
      const candidates = usable.filter((rv) => parsed.every((r) => satisfies(parseVersion(rv.version)!, r)))
      if (!candidates.length) {
        const each = parsed.every((r) => usable.some((rv) => satisfies(parseVersion(rv.version)!, r)))
        const detail = asked.map(([by, range]) => ({ requested_by: by, range }))
        return { ok: false, refusal: { code: asked.length > 1 && each ? "range_conflict" : "no_matching_version", package: pkg, ranges: detail } }
      }
      candidates.sort((a, b) => compareVersions(parseVersion(b.version)!, parseVersion(a.version)!))
      const pick = candidates[0]
      picks.set(pkg, pick)
      for (const [dep, text] of Object.entries(pick.requires.packages)) {
        const by = `${pkg}@${pick.version}`
        const recorded = ranges.get(dep) ?? new Map<string, string>()
        if (!recorded.has(by)) {
          recorded.set(by, text)
          ranges.set(dep, recorded)
          added = true
        }
      }
      queue.push(...Object.keys(pick.requires.packages).sort(byName))
    }
    if (!added) break
  }

  // Cycles: depth-first from each synthetic-root package, in name order;
  // dependencies in name order; the first back edge is reported as the path
  // that closes it.
  const stack: string[] = []
  const done = new Set<string>()
  const findCycle = (pkg: string): string[] | null => {
    const at = stack.indexOf(pkg)
    if (at >= 0) return [...stack.slice(at), pkg]
    if (done.has(pkg)) return null
    stack.push(pkg)
    for (const dep of Object.keys(picks.get(pkg)!.requires.packages).sort(byName)) {
      const cycle = findCycle(dep)
      if (cycle) return cycle
    }
    stack.pop()
    done.add(pkg)
    return null
  }
  for (const pkg of rootNames) {
    const cycle = findCycle(pkg)
    if (cycle) return { ok: false, refusal: { code: "dependency_cycle", cycle } }
  }

  // Body fields may only name packages from the same manifest's dependency
  // list, and each must resolve to the kind that field expects.
  const names = [...picks.keys()].sort(byName)
  for (const pkg of names) {
    for (const ref of picks.get(pkg)!.refs ?? []) {
      if (!Object.hasOwn(picks.get(pkg)!.requires.packages, ref.package)) return { ok: false, refusal: { code: "body_ref_missing", package: pkg, field: ref.field, ref: ref.package } }
    }
  }
  for (const pkg of names) {
    for (const ref of picks.get(pkg)!.refs ?? []) {
      const kind = picks.get(ref.package)!.kind
      if (kind !== REF_KIND[ref.field]) return { ok: false, refusal: { code: "kind_mismatch", package: pkg, field: ref.field, ref: ref.package, expected: REF_KIND[ref.field], found: kind } }
    }
  }

  const out = Object.fromEntries(names.map((pkg) => [pkg, picks.get(pkg)!.version]))
  if (!currentPins) return { ok: true, picks: out }
  const moves: Record<string, { from: string | null; to: string | null; direction: string }> = {}
  for (const pkg of [...new Set([...Object.keys(currentPins), ...names])].sort(byName)) {
    const from = currentPins[pkg] ?? null
    const to = out[pkg] ?? null
    const direction =
      from === null ? "added" : to === null ? "removed" : (["down", "same", "up"] as const)[Math.sign(compareVersions(parseVersion(to)!, parseVersion(from)!)) + 1]
    moves[pkg] = { from, to, direction }
  }
  return { ok: true, picks: out, moves }
}

function buildSyntheticRootRanges(
  registry: Record<string, RegistryVersion[]>,
  roots: Record<string, string>,
  subjectRoots: string[],
  currentPins: Record<string, string>,
): Record<string, string> {
  const rootNames = Object.keys(roots).sort(byName)
  const subjects = new Set(subjectRoots)
  for (const subject of subjects) {
    if (!Object.hasOwn(roots, subject)) throw new Error(`subject root ${subject} is not in roots`)
  }

  const pinnedClosure = (startingRoots: string[]) => {
    const reached = new Set<string>()
    const queue = [...startingRoots].sort(byName)
    while (queue.length) {
      const pkg = queue.shift()!
      if (reached.has(pkg)) continue
      reached.add(pkg)
      const pinnedVersion = currentPins[pkg]
      if (pinnedVersion === undefined) continue
      const pinned = registry[pkg]?.find((version) => version.version === pinnedVersion)
      if (!pinned) continue
      queue.push(...Object.keys(pinned.requires.packages).filter((dep) => currentPins[dep] !== undefined).sort(byName))
    }
    return reached
  }

  const nonSubjectReachable = pinnedClosure(rootNames.filter((root) => !subjects.has(root)))
  const subjectSet = new Set(subjectRoots)
  for (const pkg of Object.keys(currentPins)) {
    if (!Object.hasOwn(roots, pkg) && !nonSubjectReachable.has(pkg)) subjectSet.add(pkg)
  }

  const rootRanges: Record<string, string> = {}
  for (const root of rootNames) {
    if (subjects.has(root)) {
      rootRanges[root] = roots[root]
    } else {
      const pinnedVersion = currentPins[root]
      if (pinnedVersion === undefined) throw new Error(`non-subject root ${root} has no current pin`)
      rootRanges[root] = `=${pinnedVersion}`
    }
  }
  for (const pkg of Object.keys(currentPins).sort(byName)) {
    if (!subjectSet.has(pkg)) rootRanges[pkg] = `=${currentPins[pkg]}`
  }
  return Object.fromEntries(Object.entries(rootRanges).sort(([a], [b]) => byName(a, b)))
}

function resolveSeveralRoots(
  registry: Record<string, RegistryVersion[]>,
  roots: Record<string, string>,
  subjectRoots: string[],
  currentPins: Record<string, string>,
  localCapabilities: string[],
): { rootRanges: Record<string, string>; result: Resolution } {
  const rootRanges = buildSyntheticRootRanges(registry, roots, subjectRoots, currentPins)
  const result = resolve(registry, { packages: rootRanges }, localCapabilities, currentPins)
  if (result.ok || (result.refusal.code !== "range_conflict" && result.refusal.code !== "no_matching_version")) return { rootRanges, result }

  const pkg = result.refusal.package
  if (typeof pkg !== "string") return { rootRanges, result }
  const pinnedVersion = currentPins[pkg]
  if (pinnedVersion === undefined || rootRanges[pkg] !== `=${pinnedVersion}`) return { rootRanges, result }
  const pinned = parseVersion(pinnedVersion)
  const constraints = Array.isArray(result.refusal.ranges) ? result.refusal.ranges as { requested_by: string; range: string }[] : []
  const conflict = constraints.find(({ requested_by, range }) => {
    if (requested_by === "(root)" && range === `=${pinnedVersion}`) return false
    const parsed = parseRange(range)
    return pinned !== null && parsed !== null && !satisfies(pinned, parsed)
  })
  if (!conflict) return { rootRanges, result }
  return {
    rootRanges,
    result: {
      ok: false,
      refusal: { code: "pin_conflict", package: pkg, pinned_version: pinnedVersion, range: conflict.range, requested_by: conflict.requested_by },
    },
  }
}

function resolverVectors() {
  const rv = (version: string, o: { kind?: Kind; status?: State; caps?: string[]; deps?: Record<string, string>; refs?: { field: RefField; package: string }[] } = {}): RegistryVersion => ({
    version,
    kind: o.kind ?? "persona",
    status: o.status ?? "available",
    requires: { capabilities: o.caps ?? [], packages: o.deps ?? {} },
    ...(o.refs ? { refs: o.refs } : {}),
  })

  const rangeCases: [string, string, boolean][] = [
    ["^1", "1.0.0", true], ["^1", "1.9.3", true], ["^1", "2.0.0", false], ["^1", "0.9.0", false],
    ["^1.2", "1.2.0", true], ["^1.2", "1.1.9", false],
    ["^1.2.3", "1.2.3", true], ["^1.2.3", "1.2.2", false], ["^1.2.3", "1.99.0", true],
    ["^0.2.3", "0.2.9", true], ["^0.2.3", "0.3.0", false],
    ["^0.0.3", "0.0.3", true], ["^0.0.3", "0.0.4", false],
    ["^0.0", "0.0.9", true], ["^0.0", "0.1.0", false],
    ["^0", "0.9.9", true], ["^0", "1.0.0", false],
    ["~1", "1.9.0", true], ["~1.2", "1.2.9", true], ["~1.2", "1.3.0", false], ["~1.2.3", "1.2.2", false], ["~1.2.3", "1.2.9", true],
    ["1", "1.5.0", true], ["1.2", "1.2.7", true], ["1.2", "1.3.0", false], ["1.2.3", "1.2.3", true], ["1.2.3", "1.2.4", false], ["=1.2.3", "1.2.3", true], ["=1.2", "1.2.5", true],
    [">=1.2.3", "2.0.0", true], [">1.2.3", "1.2.3", false], [">1.2", "1.2.9", false], [">1.2", "1.3.0", true],
    ["<1.2", "1.1.9", true], ["<1.2", "1.2.0", false], ["<=1.2", "1.2.9", true], ["<=1.2", "1.3.0", false], ["<=1.2.3", "1.2.3", true], ["<=1.2.3", "1.2.4", false],
    [">=1.2.0 <1.4.0", "1.3.9", true], [">=1.2.0 <1.4.0", "1.4.0", false],
    ["^1 || ^3", "3.1.0", true], ["^1 || ^3", "2.0.0", false], ["^1||^3", "1.0.0", true],
    ["*", "5.0.0", true], ["*", "0.0.0", true], ["*", "1.0.0-beta.1", false],
    ["^1", "1.3.0-beta.1", false], ["<1.3.0", "1.3.0-beta.1", false], ["1.3.0", "1.3.0-beta.1", false],
    ["1.3.0-beta.1", "1.3.0-beta.1", true], ["=1.3.0-beta.1", "1.3.0-beta.1", true], ["1.3.0-beta.1", "1.3.0-beta.2", false], ["1.3.0-beta.1", "1.3.0", false],
    ["1.3.0-beta.1 || ^2", "1.3.0-beta.1", true],
  ]
  const ranges = rangeCases.map(([range, version, expected]) => {
    const r = parseRange(range)
    const ver = parseVersion(version)
    if (!r || !ver) throw new Error(`range vector ${range} ${version} does not parse`)
    if (satisfies(ver, r) !== expected) throw new Error(`range vector ${range} ${version}: expected ${expected}`)
    return { range, version, satisfies: expected }
  })
  const invalidRanges = ["", " ", "latest", "^", "1.x", "1.2.*", "v1.2.3", ">= 1.2.3", "1.2 - 1.4", "^1.2.3-beta.1", ">=1.0.0-rc.1", "~1.2.3-beta", "1.3.0-beta.1 >=1.0.0", "1.2.3+build", "01.2.3", "* >1", "1 ||"].map((range) => {
    if (parseRange(range) !== null) throw new Error(`range ${JSON.stringify(range)} should be invalid`)
    return { range, code: "range_invalid" }
  })
  const precedence = ["1.0.0-alpha", "1.0.0-alpha.1", "1.0.0-alpha.beta", "1.0.0-beta", "1.0.0-beta.2", "1.0.0-beta.11", "1.0.0-rc.1", "1.0.0", "1.0.1", "1.1.0", "1.10.0", "2.0.0"]
  for (let i = 1; i < precedence.length; i++) {
    if (compareVersions(parseVersion(precedence[i - 1])!, parseVersion(precedence[i])!) >= 0) throw new Error(`precedence ${precedence[i]}`)
  }
  const invalidVersions = ["1.0", "1", "1.0.0+build.1", "01.0.0", "1.0.0-01", "v1.0.0", "1.0.0-", "1.0.0-beta..1", "1.0.0 "].map((version) => {
    if (parseVersion(version) !== null) throw new Error(`version ${version} should be invalid`)
    return { version, code: "version_invalid" }
  })

  type Case = {
    name: string
    note: string
    local_capabilities: string[]
    registry: Record<string, RegistryVersion[]>
    expect: Resolution
  } & (
    | { root: { package: string; range: string }; current_pins?: Record<string, string> }
    | {
        roots: Record<string, string>
        subject_roots: string[]
        current_pins: Record<string, string>
        synthetic_root: { requires: { packages: Record<string, string> } }
      }
  )
  const head = "@x/head"
  const caps: string[] = []
  const cases: Case[] = [
    { name: "highest_in_range", note: "", local_capabilities: caps, registry: { [head]: [rv("1.0.0"), rv("1.2.0"), rv("2.0.0")] }, root: { package: head, range: "^1" }, expect: { ok: true, picks: { [head]: "1.2.0" } } },
    {
      name: "withdrawn_skipped",
      note: "",
      local_capabilities: caps,
      registry: { [head]: [rv("1.1.0"), rv("1.2.0", { status: "withdrawn" })] },
      root: { package: head, range: "^1" },
      expect: { ok: true, picks: { [head]: "1.1.0" } },
    },
    {
      name: "tombstoned_and_revoked_skipped",
      note: "",
      local_capabilities: caps,
      registry: { [head]: [rv("1.1.0"), rv("1.2.0", { status: "revoked" }), rv("1.3.0", { status: "tombstoned" })] },
      root: { package: head, range: "^1" },
      expect: { ok: true, picks: { [head]: "1.1.0" } },
    },
    {
      name: "exact_range_on_withdrawn_version",
      note: "resolution skips a withdrawn version even when the range names it exactly; an existing pin to it keeps working, outside resolution",
      local_capabilities: caps,
      registry: { [head]: [rv("1.1.0"), rv("1.2.0", { status: "withdrawn" })] },
      root: { package: head, range: "1.2.0" },
      expect: { ok: false, refusal: { code: "no_matching_version", package: head, ranges: [{ requested_by: "(root)", range: "1.2.0" }] } },
    },
    { name: "prerelease_not_picked_by_range", note: "", local_capabilities: caps, registry: { [head]: [rv("1.2.0"), rv("1.3.0-beta.1")] }, root: { package: head, range: "^1" }, expect: { ok: true, picks: { [head]: "1.2.0" } } },
    {
      name: "prerelease_named_exactly",
      note: "",
      local_capabilities: caps,
      registry: { [head]: [rv("1.2.0"), rv("1.3.0-beta.1")] },
      root: { package: head, range: "1.3.0-beta.1" },
      expect: { ok: true, picks: { [head]: "1.3.0-beta.1" } },
    },
    {
      name: "capability_filter",
      note: "1.3.0 needs a token no local module declares; tokens match by exact string",
      local_capabilities: ["cap/flow.template@1"],
      registry: { [head]: [rv("1.2.0", { caps: ["cap/flow.template@1"] }), rv("1.3.0", { caps: ["cap/flow.template@2"] })] },
      root: { package: head, range: "^1" },
      expect: { ok: true, picks: { [head]: "1.2.0" } },
    },
    {
      name: "capability_unmet",
      note: "",
      local_capabilities: ["cap/flow.template@1"],
      registry: { [head]: [rv("1.3.0", { caps: ["cap/flow.template@2"] })] },
      root: { package: head, range: "^1" },
      expect: { ok: false, refusal: { code: "no_matching_version", package: head, ranges: [{ requested_by: "(root)", range: "^1" }] } },
    },
    {
      name: "persona_closure",
      note: "every body reference is in requires.packages and resolves to the kind its field expects",
      local_capabilities: caps,
      registry: {
        [head]: [
          rv("1.0.0", {
            deps: { "@x/base": "^1", "@x/skill-a": "^2", "@x/flow-a": "^4", "@x/mason": "^1", "@x/memory": "^1" },
            refs: [
              { field: "extends", package: "@x/base" },
              { field: "skills", package: "@x/skill-a" },
              { field: "flows", package: "@x/flow-a" },
              { field: "delegates_to", package: "@x/mason" },
              { field: "memory_profile", package: "@x/memory" },
            ],
          }),
        ],
        "@x/base": [rv("1.0.0"), rv("1.4.1")],
        "@x/skill-a": [rv("2.0.0", { kind: "skill" }), rv("2.1.0", { kind: "skill" }), rv("3.0.0", { kind: "skill" })],
        "@x/flow-a": [rv("4.2.0", { kind: "flow" })],
        "@x/mason": [rv("1.0.0")],
        "@x/memory": [rv("1.0.0", { kind: "memory-profile" })],
      },
      root: { package: head, range: "^1" },
      expect: { ok: true, picks: { "@x/base": "1.4.1", "@x/flow-a": "4.2.0", [head]: "1.0.0", "@x/mason": "1.0.0", "@x/memory": "1.0.0", "@x/skill-a": "2.1.0" } },
    },
    {
      name: "shared_dependency_intersection",
      note: "one version per package: c must satisfy both ^1.2 and ~1.3",
      local_capabilities: caps,
      registry: {
        [head]: [rv("1.0.0", { deps: { "@x/a": "^1", "@x/b": "^1" } })],
        "@x/a": [rv("1.0.0", { deps: { "@x/c": "^1.2" } })],
        "@x/b": [rv("1.0.0", { deps: { "@x/c": "~1.3" } })],
        "@x/c": [rv("1.2.0"), rv("1.3.0"), rv("1.3.5"), rv("1.4.0")],
      },
      root: { package: head, range: "^1" },
      expect: { ok: true, picks: { "@x/a": "1.0.0", "@x/b": "1.0.0", "@x/c": "1.3.5", [head]: "1.0.0" } },
    },
    {
      name: "conflicting_ranges",
      note: "each range alone has a version; together none: refused by name",
      local_capabilities: caps,
      registry: {
        [head]: [rv("1.0.0", { deps: { "@x/a": "^1", "@x/b": "^1" } })],
        "@x/a": [rv("1.0.0", { deps: { "@x/c": "^1" } })],
        "@x/b": [rv("1.0.0", { deps: { "@x/c": "^2" } })],
        "@x/c": [rv("1.0.0"), rv("2.0.0")],
      },
      root: { package: head, range: "^1" },
      expect: {
        ok: false,
        refusal: { code: "range_conflict", package: "@x/c", ranges: [{ requested_by: "@x/a@1.0.0", range: "^1" }, { requested_by: "@x/b@1.0.0", range: "^2" }] },
      },
    },
    {
      name: "later_range_moves_pick_down",
      note: "a@1.1.0 asks for c ~1.0, so c resolves to 1.0.5, not the highest ^1 version",
      local_capabilities: caps,
      registry: {
        [head]: [rv("1.0.0", { deps: { "@x/a": "^1", "@x/c": "^1" } })],
        "@x/a": [rv("1.0.0"), rv("1.1.0", { deps: { "@x/c": "~1.0" } })],
        "@x/c": [rv("1.0.5"), rv("1.1.0")],
      },
      root: { package: head, range: "^1" },
      expect: { ok: true, picks: { "@x/a": "1.1.0", "@x/c": "1.0.5", [head]: "1.0.0" } },
    },
    {
      name: "superseded_ranges_kept",
      note: "b@1.1.0 asked for a <1.1 before b moved down; that range stays, so a ends at 1.0.0 although a backtracking solver could pick a 1.1.0",
      local_capabilities: caps,
      registry: {
        [head]: [rv("1.0.0", { deps: { "@x/a": "^1", "@x/b": "^1" } })],
        "@x/a": [rv("1.0.0", { deps: { "@x/b": "<1.1" } }), rv("1.1.0")],
        "@x/b": [rv("1.0.0"), rv("1.1.0", { deps: { "@x/a": "<1.1" } })],
      },
      root: { package: head, range: "^1" },
      expect: { ok: true, picks: { "@x/a": "1.0.0", "@x/b": "1.0.0", [head]: "1.0.0" } },
    },
    {
      name: "cycle",
      note: "",
      local_capabilities: caps,
      registry: { [head]: [rv("1.0.0", { deps: { "@x/a": "^1" } })], "@x/a": [rv("1.0.0", { deps: { "@x/b": "^1" } })], "@x/b": [rv("1.0.0", { deps: { "@x/a": "^1" } })] },
      root: { package: head, range: "^1" },
      expect: { ok: false, refusal: { code: "dependency_cycle", cycle: ["@x/a", "@x/b", "@x/a"] } },
    },
    {
      name: "self_dependency",
      note: "",
      local_capabilities: caps,
      registry: { [head]: [rv("1.0.0", { deps: { [head]: "^1" } })] },
      root: { package: head, range: "^1" },
      expect: { ok: false, refusal: { code: "dependency_cycle", cycle: [head, head] } },
    },
    {
      name: "unknown_package",
      note: "",
      local_capabilities: caps,
      registry: { [head]: [rv("1.0.0", { deps: { "@x/missing": "^1" } })] },
      root: { package: head, range: "^1" },
      expect: { ok: false, refusal: { code: "package_not_found", package: "@x/missing" } },
    },
    {
      name: "invalid_dependency_range",
      note: "",
      local_capabilities: caps,
      registry: { [head]: [rv("1.0.0", { deps: { "@x/a": "1.x" } })], "@x/a": [rv("1.0.0")] },
      root: { package: head, range: "^1" },
      expect: { ok: false, refusal: { code: "range_invalid", package: "@x/a", range: "1.x", requested_by: "@x/head@1.0.0" } },
    },
    {
      name: "invalid_root_range",
      note: "",
      local_capabilities: caps,
      registry: { [head]: [rv("1.0.0")] },
      root: { package: head, range: "latest" },
      expect: { ok: false, refusal: { code: "range_invalid", package: head, range: "latest", requested_by: "(root)" } },
    },
    {
      name: "body_ref_not_in_requires",
      note: "a body field names a package the dependency list lacks",
      local_capabilities: caps,
      registry: { [head]: [rv("1.0.0", { refs: [{ field: "skills", package: "@x/skill-a" }] })], "@x/skill-a": [rv("1.0.0", { kind: "skill" })] },
      root: { package: head, range: "^1" },
      expect: { ok: false, refusal: { code: "body_ref_missing", package: head, field: "skills", ref: "@x/skill-a" } },
    },
    {
      name: "body_ref_wrong_kind",
      note: "skills names a persona package",
      local_capabilities: caps,
      registry: { [head]: [rv("1.0.0", { deps: { "@x/skill-a": "^1" }, refs: [{ field: "skills", package: "@x/skill-a" }] })], "@x/skill-a": [rv("1.0.0")] },
      root: { package: head, range: "^1" },
      expect: { ok: false, refusal: { code: "kind_mismatch", package: head, field: "skills", ref: "@x/skill-a", expected: "skill", found: "persona" } },
    },
    {
      name: "downgrade_after_withdrawal",
      note: "the pinned 1.3.0 was withdrawn; an explicit upgrade re-resolves and the pin moves down",
      local_capabilities: caps,
      registry: {
        [head]: [rv("1.2.0", { deps: { "@x/a": "^1" } }), rv("1.3.0", { status: "withdrawn", deps: { "@x/a": "~1.1" } })],
        "@x/a": [rv("1.1.0"), rv("1.2.0")],
      },
      root: { package: head, range: "^1" },
      current_pins: { [head]: "1.3.0", "@x/a": "1.1.0" },
      expect: {
        ok: true,
        picks: { "@x/a": "1.2.0", [head]: "1.2.0" },
        moves: { "@x/a": { from: "1.1.0", to: "1.2.0", direction: "up" }, [head]: { from: "1.3.0", to: "1.2.0", direction: "down" } },
      },
    },
    {
      name: "dependency_dropped",
      note: "",
      local_capabilities: caps,
      registry: { [head]: [rv("1.0.0", { deps: { "@x/old": "^1" } }), rv("1.1.0")], "@x/old": [rv("1.0.0")] },
      root: { package: head, range: "^1" },
      current_pins: { [head]: "1.0.0", "@x/old": "1.0.0" },
      expect: { ok: true, picks: { [head]: "1.1.0" }, moves: { [head]: { from: "1.0.0", to: "1.1.0", direction: "up" }, "@x/old": { from: "1.0.0", to: null, direction: "removed" } } },
    },
    {
      name: "install_preserves_existing_pins",
      note: "installing B pins existing root A and shared dependency D exactly, while B and its new dependency E are added",
      local_capabilities: caps,
      registry: {
        "@x/a": [rv("1.0.0", { deps: { "@x/d": "^1.0.0" } })],
        "@x/b": [rv("1.0.0", { deps: { "@x/d": "^1.0.0", "@x/e": "^1.0.0" } })],
        "@x/d": [rv("1.0.0"), rv("1.1.0")],
        "@x/e": [rv("1.0.0")],
      },
      roots: { "@x/a": "^1.0.0", "@x/b": "^1.0.0" },
      subject_roots: ["@x/b"],
      current_pins: { "@x/a": "1.0.0", "@x/d": "1.0.0" },
      synthetic_root: { requires: { packages: { "@x/a": "=1.0.0", "@x/b": "^1.0.0", "@x/d": "=1.0.0" } } },
      expect: {
        ok: true,
        picks: { "@x/a": "1.0.0", "@x/b": "1.0.0", "@x/d": "1.0.0", "@x/e": "1.0.0" },
        moves: {
          "@x/a": { from: "1.0.0", to: "1.0.0", direction: "same" },
          "@x/b": { from: null, to: "1.0.0", direction: "added" },
          "@x/d": { from: "1.0.0", to: "1.0.0", direction: "same" },
          "@x/e": { from: null, to: "1.0.0", direction: "added" },
        },
      },
    },
    {
      name: "install_pin_conflict",
      note: "B's D range conflicts with D's exact installed pin, so install refuses instead of moving D",
      local_capabilities: caps,
      registry: {
        "@x/a": [rv("1.0.0", { deps: { "@x/d": "^1.0.0" } })],
        "@x/b": [rv("1.0.0", { deps: { "@x/d": "^1.1.0", "@x/e": "^1.0.0" } })],
        "@x/d": [rv("1.0.0"), rv("1.1.0")],
        "@x/e": [rv("1.0.0")],
      },
      roots: { "@x/a": "^1.0.0", "@x/b": "^1.0.0" },
      subject_roots: ["@x/b"],
      current_pins: { "@x/a": "1.0.0", "@x/d": "1.0.0" },
      synthetic_root: { requires: { packages: { "@x/a": "=1.0.0", "@x/b": "^1.0.0", "@x/d": "=1.0.0" } } },
      expect: {
        ok: false,
        refusal: { code: "pin_conflict", package: "@x/d", pinned_version: "1.0.0", range: "^1.1.0", requested_by: "@x/b@1.0.0" },
      },
    },
    {
      name: "named_upgrade_keeps_shared_dependency_pinned",
      note: "upgrading A lets A move, but C also reaches D so D remains pinned at 1.0.0",
      local_capabilities: caps,
      registry: {
        "@x/a": [rv("1.0.0", { deps: { "@x/d": "^1.0.0" } }), rv("1.1.0", { deps: { "@x/d": "^1.0.0" } })],
        "@x/c": [rv("1.0.0", { deps: { "@x/d": "^1.0.0" } })],
        "@x/d": [rv("1.0.0"), rv("1.1.0")],
      },
      roots: { "@x/a": "^1.0.0", "@x/c": "^1.0.0" },
      subject_roots: ["@x/a"],
      current_pins: { "@x/a": "1.0.0", "@x/c": "1.0.0", "@x/d": "1.0.0" },
      synthetic_root: { requires: { packages: { "@x/a": "^1.0.0", "@x/c": "=1.0.0", "@x/d": "=1.0.0" } } },
      expect: {
        ok: true,
        picks: { "@x/a": "1.1.0", "@x/c": "1.0.0", "@x/d": "1.0.0" },
        moves: {
          "@x/a": { from: "1.0.0", to: "1.1.0", direction: "up" },
          "@x/c": { from: "1.0.0", to: "1.0.0", direction: "same" },
          "@x/d": { from: "1.0.0", to: "1.0.0", direction: "same" },
        },
      },
    },
    {
      name: "upgrade_all_moves_shared_dependency",
      note: "both roots are subjects, so no root outside the upgrade pins shared dependency D",
      local_capabilities: caps,
      registry: {
        "@x/a": [rv("1.0.0", { deps: { "@x/d": "^1.0.0" } }), rv("1.1.0", { deps: { "@x/d": "^1.0.0" } })],
        "@x/c": [rv("1.0.0", { deps: { "@x/d": "^1.0.0" } })],
        "@x/d": [rv("1.0.0"), rv("1.1.0")],
      },
      roots: { "@x/a": "^1.0.0", "@x/c": "^1.0.0" },
      subject_roots: ["@x/a", "@x/c"],
      current_pins: { "@x/a": "1.0.0", "@x/c": "1.0.0", "@x/d": "1.0.0" },
      synthetic_root: { requires: { packages: { "@x/a": "^1.0.0", "@x/c": "^1.0.0" } } },
      expect: {
        ok: true,
        picks: { "@x/a": "1.1.0", "@x/c": "1.0.0", "@x/d": "1.1.0" },
        moves: {
          "@x/a": { from: "1.0.0", to: "1.1.0", direction: "up" },
          "@x/c": { from: "1.0.0", to: "1.0.0", direction: "same" },
          "@x/d": { from: "1.0.0", to: "1.1.0", direction: "up" },
        },
      },
    },
  ]
  for (const c of cases) {
    let got: Resolution
    if ("root" in c) {
      got = resolve(c.registry, c.root, c.local_capabilities, c.current_pins)
    } else {
      const { rootRanges, result } = resolveSeveralRoots(c.registry, c.roots, c.subject_roots, c.current_pins, c.local_capabilities)
      const expectedRanges = c.synthetic_root.requires.packages
      if (JSON.stringify(rootRanges) !== JSON.stringify(expectedRanges)) {
        throw new Error(`resolver case ${c.name}: synthetic root ranges ${JSON.stringify(rootRanges)} did not match ${JSON.stringify(expectedRanges)}`)
      }
      got = result
    }
    if (JSON.stringify(got) !== JSON.stringify(c.expect)) throw new Error(`resolver case ${c.name}: got ${JSON.stringify(got)}`)
  }
  const tokenValid = ["cap/code.read@1", "cap/shell@1", "cap/flow.template@2", "cap/code.read@10"]
  const tokenInvalid = ["code.read@1", "cap/code.read", "cap/code.read@0", "cap/code.read@01", "cap/Code.read@1", "cap/code.read.all@1", "cap/code_read@1", "flow-templates/v1", "cap/.read@1", "cap/code.@1", "cap/memory-profile.read@1", "cap/code-x.read@1", "cap/code.re-ad@1", "cap/1code.read@1", "cap/code.1read@1"]
  for (const t of tokenValid) if (!CAPABILITY_TOKEN.test(t)) throw new Error(`token ${t} should be valid`)
  for (const t of tokenInvalid) if (CAPABILITY_TOKEN.test(t)) throw new Error(`token ${t} should be invalid`)
  for (const c of cases) for (const t of [...c.local_capabilities, ...Object.values(c.registry).flatMap((vs) => vs.flatMap((x) => x.requires.capabilities))]) {
    if (!CAPABILITY_TOKEN.test(t)) throw new Error(`resolver case ${c.name}: token ${t} breaks the grammar`)
  }
  const capabilityPairs = [
    { token: "cap/code.read@1", capability: "cap-code-read/v1" },
    { token: "cap/shell@1", capability: "cap-shell/v1" },
    { token: "cap/flow.template@2", capability: "cap-flow-template/v2" },
    { token: "cap/code.read@10", capability: "cap-code-read/v10" },
    { token: `cap/${"a".repeat(60)}@1`, capability: `cap-${"a".repeat(60)}/v1` },
  ]
  const tokenTooLong = `cap/${"a".repeat(61)}@1`
  const capabilityNotAToken = ["cap-a-b-c/v1", "cap-/v1", "cap-code-/v1", "flow-scopes/v1", "cap-code-read/v0", "cap-Code-read/v1"]
  for (const { token, capability } of capabilityPairs) {
    const name = capability.slice(0, capability.indexOf("/v"))
    if (capabilityTokenToManifest(token) !== capability) throw new Error(`capability mapping ${token} should produce ${capability}`)
    if (capabilityManifestToToken(capability) !== token) throw new Error(`capability mapping ${capability} should produce ${token}`)
    if (!SUBC_CAPABILITY_NAME.test(capability) || name.length > CAPABILITY_NAME_MAX) {
      throw new Error(`capability mapping ${capability} does not satisfy the subc name rule`)
    }
  }
  const tooLongName = `cap-${"a".repeat(61)}`
  if (!CAPABILITY_TOKEN.test(tokenTooLong) || tooLongName.length !== 65 || capabilityTokenToManifest(tokenTooLong) !== null) {
    throw new Error(`capability token ${tokenTooLong} should be refused for exceeding the manifest name limit`)
  }
  if (capabilityManifestToToken(`${tooLongName}/v1`) !== null) {
    throw new Error("capability manifest name over 64 characters should not map to a token")
  }
  for (const capability of capabilityNotAToken) {
    if (capabilityManifestToToken(capability) !== null) throw new Error(`capability ${capability} should not map to a token`)
  }
  return {
    schema: "ck-catalog-v1/resolver",
    rules: {
      versions: "SemVer 2.0.0 without build metadata",
      capability_token: CAPABILITY_TOKEN.source,
      range_grammar: 'range := set ("||" set)*; set := comparator (whitespace comparator)*; comparator := "*" | op? partial; op := ^ ~ >= <= > < =; partial := N | N.N | N.N.N | N.N.N-prerelease',
      prerelease: "a prerelease version satisfies a range only when one ||-set is exactly that version (bare or =); a prerelease anywhere else in a range is range_invalid",
      eligibility: "status available, and every requires.capabilities token is declared by a local module (exact string match)",
      algorithm: "in passes: walk breadth-first from the root, dependencies in name order; pick each package once per pass as the highest eligible version satisfying every range recorded for it so far, then record that version's requires.packages ranges (keyed by requester name@version). Stop after a pass that records nothing new. Recorded ranges are never removed. Then check cycles, then body references.",
      pin_stability: "the synthetic root requires every current pin at =version except subject roots and pinned dependencies no non-subject root reaches; install subjects only the new root, named upgrade subjects that root, and upgrade all subjects every root. A conflicting exact pin refuses pin_conflict.",
      refusal_detail: "ranges are listed sorted by requested_by; '(root)' is the spec's own range; other requesters are name@version",
      picks_order: "picks and moves are objects keyed by package name in ascending order",
    },
    ranges,
    invalid_ranges: invalidRanges,
    precedence_ascending: precedence,
    invalid_versions: invalidVersions,
    capability_tokens: { valid: tokenValid, invalid: tokenInvalid },
    capability_manifest_mapping: { pairs: capabilityPairs, token_too_long: tokenTooLong, capability_not_a_token: capabilityNotAToken },
    cases,
  }
}

// ───────────────────────────── files ─────────────────────────────

export const JSON_FILES = ["names.json", "record.json", "archive.json", "status.json", "statements.json", "resolver.json", "starter-bundle.json"] as const
const README = "README.md"
const SUMS_BEGIN = "<!-- sha256:begin (generated by generate.ts) -->"
const SUMS_END = "<!-- sha256:end -->"

function asJson(value: unknown): string {
  return `${JSON.stringify(value, null, 2)}\n`
}

// The README is hand-written prose except for the block between the two
// markers, which lists each vector file's SHA-256 and is rewritten here.
export function generateFiles(readmeText: string): Record<string, string> {
  const files: Record<string, string> = {
    "names.json": asJson(namesVectors()),
    "record.json": asJson(recordVectors()),
    "archive.json": asJson(archiveVectors()),
    "status.json": asJson(statusVectors()),
    "statements.json": asJson(statementsVectors()),
    "resolver.json": asJson(resolverVectors()),
    "starter-bundle.json": asJson(starterBundleVectors()),
  }
  const begin = readmeText.indexOf(SUMS_BEGIN)
  const end = readmeText.indexOf(SUMS_END)
  if (begin < 0 || end < begin) throw new Error(`${README} lacks the sha256 markers`)
  const table = ["", "| File | sha256 |", "| --- | --- |", ...JSON_FILES.map((name) => `| ${name} | \`${sha256Hex(files[name])}\` |`), ""].join("\n")
  files[README] = readmeText.slice(0, begin + SUMS_BEGIN.length) + table + readmeText.slice(end)
  return files
}

// Returns the names of committed files whose bytes differ from what the
// generator produces now. `read` returns a committed file's text by name.
export function driftedFiles(read: (name: string) => string = (name) => readFileSync(join(VECTOR_DIR, name), "utf8")): string[] {
  const expected = generateFiles(read(README))
  return Object.keys(expected).filter((name) => {
    try {
      return read(name) !== expected[name]
    } catch {
      return true
    }
  })
}

if (import.meta.main) {
  if (process.argv.includes("--check")) {
    const drifted = driftedFiles()
    if (drifted.length) {
      console.error(`ck-catalog-v1 vectors drifted from generate.ts: ${drifted.join(", ")}`)
      process.exit(1)
    }
    console.log(`ck-catalog-v1 vectors match generate.ts (${JSON_FILES.length + 1} files)`)
  } else {
    const files = generateFiles(readFileSync(join(VECTOR_DIR, README), "utf8"))
    for (const [name, text] of Object.entries(files)) writeFileSync(join(VECTOR_DIR, name), text)
    console.log(`wrote ${Object.keys(files).join(", ")}`)
  }
}

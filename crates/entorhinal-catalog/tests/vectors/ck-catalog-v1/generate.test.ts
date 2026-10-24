import { describe, expect, test } from "bun:test"
import { readFileSync } from "node:fs"
import { join } from "node:path"
import { driftedFiles, DOMAIN_STARTER_BUNDLE, JSON_FILES, jcs, parseRange, parseStrictJson, parseVersion, resolve, satisfies, sha256Hex, signingInput, SKILL_PACKAGE, TEST_KEY_1, VECTOR_DIR, verifyRecord } from "./generate"

const committed = (name: string) => readFileSync(join(VECTOR_DIR, name), "utf8")

describe("ck-catalog-v1 JSON number tokens", () => {
  test("rejects fractional, exponent and negative-zero tokens", () => {
    for (const token of ["1.0", "1e2", "1E0", "0e0", "-0"]) {
      expect(() => parseStrictJson(`{"value":${token}}`)).toThrow()
    }
  })

  test("accepts canonical safe integer tokens", () => {
    for (const token of ["-9007199254740991", "-1", "0", "1", "100", "9007199254740991"]) {
      expect(parseStrictJson(`{"value":${token}}`)).toEqual({ value: Number(token) })
    }
  })

  test("preserves number-like strings", () => {
    const strings = ["1.0", "1e2", "1E0", "0e0", "-0"]
    expect(parseStrictJson(JSON.stringify(strings))).toEqual(strings)
  })

  test("number-token record vectors refuse before signature checks", () => {
    const records = JSON.parse(committed("record.json")) as {
      valid: { name: string; canonical: string; signature: string }[]
      cases: { name: string; served_text: string; signature: string; expect: unknown }[]
    }
    const base = records.valid.find((record) => record.name === "skill_record")!
    const trusted = [TEST_KEY_1.fingerprint]
    expect(verifyRecord(base.canonical, base.signature, SKILL_PACKAGE, trusted)).toEqual({ ok: true })
    const cases = records.cases.filter((record) => record.name.startsWith("json_number_"))
    const tokens = ["1.0", "1e2", "1E0", "0e0", "-0"]
    expect(cases.map((record) => record.served_text.match(/"archive_bytes":([^,}]+)/)![1])).toEqual(tokens)
    for (const [index, record] of cases.entries()) {
      expect(JSON.parse(record.served_text)).toEqual({ ...JSON.parse(base.canonical), archive_bytes: Number(tokens[index]) })
      expect(record.signature).toBe(base.signature)
      expect(record.expect).toEqual({ ok: false, code: "record_invalid", field: "json" })
      expect(verifyRecord(record.served_text, record.signature, SKILL_PACKAGE, trusted)).toEqual({ ok: false, code: "record_invalid", field: "json" })
      expect(verifyRecord(record.served_text, "0".repeat(128), SKILL_PACKAGE, [])).toEqual({ ok: false, code: "record_invalid", field: "json" })
    }
  })
})

describe("ck-catalog-v1 test vectors", () => {
  // Regenerating recomputes every hash and signature and re-derives every
  // expected outcome with the reference implementations, so this also fails
  // when a hand-written expectation stops matching the reference code.
  test("committed vector files match generate.ts byte for byte", () => {
    expect(driftedFiles()).toEqual([])
  })

  test("the drift check reports a changed file by name", () => {
    for (const name of JSON_FILES) {
      const edited = (file: string) => (file === name ? `${committed(file)} ` : committed(file))
      expect(driftedFiles(edited)).toEqual([name])
    }
    // The README is its own template outside the checksum block, so a stale
    // checksum is the README drift that matters.
    const staleSum = (file: string) => (file === "README.md" ? committed(file).replace(/(\| names\.json \| `)[0-9a-f]/, "$1x") : committed(file))
    expect(staleSum("README.md")).not.toBe(committed("README.md"))
    expect(driftedFiles(staleSum)).toEqual(["README.md"])
  })
})

describe("ck-catalog-v1 starter bundle vector", () => {
  test("includes deterministic package archives and JCS signing input", () => {
    const vector = JSON.parse(committed("starter-bundle.json")) as {
      domain: string
      valid: {
        bundle_json: { records: { record: string; record_member_sha256: string }[] }
        bundle_json_jcs: string
        bundle_signing_input_hex: string
        bundle_archive_base64: string
        bundle_archive_sha256: string
        packages: { kind: string; record_path: string; record_file: unknown; archive_base64: string; archive_sha256: string; record: { canonical: string; signing_input_hex: string }; status: { canonical: string; signing_input_hex: string } }[]
      }
      cases: { name: string; bundle_archive_base64: string; bundle_archive_sha256: string; expect: { ok: boolean; code?: string; field?: string; rule?: string; member?: string; offset?: number } }[]
      persona_manifest_cases: { name: string; manifest_text: string; expect: { ok: boolean; code?: string } }[]
    }
    expect(vector.domain).toBe(DOMAIN_STARTER_BUNDLE)
    expect(jcs(JSON.parse(vector.valid.bundle_json_jcs))).toBe(vector.valid.bundle_json_jcs)
    expect(signingInput(DOMAIN_STARTER_BUNDLE, vector.valid.bundle_json_jcs).toString("hex")).toBe(vector.valid.bundle_signing_input_hex)
    expect(sha256Hex(Buffer.from(vector.valid.bundle_archive_base64, "base64"))).toBe(vector.valid.bundle_archive_sha256)
    expect(vector.valid.packages.map((pkg) => pkg.kind)).toEqual(["persona", "skill"])
    expect(vector.persona_manifest_cases).toHaveLength(1)
    const personaManifestCase = vector.persona_manifest_cases[0]
    expect(personaManifestCase.name).toBe("persona_body_memory_scope")
    expect(JSON.parse(personaManifestCase.manifest_text).body.memory_scope).toBe("project")
    expect(personaManifestCase.expect).toEqual({ ok: false, code: "manifest_invalid" })
    const recordRows = new Map(vector.valid.bundle_json.records.map((row) => [row.record, row]))
    for (const pkg of vector.valid.packages) {
      expect(sha256Hex(Buffer.from(pkg.archive_base64, "base64"))).toBe(pkg.archive_sha256)
      expect(recordRows.get(pkg.record_path)?.record_member_sha256).toBe(sha256Hex(jcs(pkg.record_file)))
      expect(pkg.record.signing_input_hex).toBe(signingInput("ck-record/v1", pkg.record.canonical).toString("hex"))
      expect(pkg.status.signing_input_hex).toBe(signingInput("ck-status/v1", pkg.status.canonical).toString("hex"))
    }
    const cases = new Map(vector.cases.map((entry) => [entry.name, entry]))
    expect(cases.get("bad_bundle_signature")?.expect).toEqual({ ok: false, code: "signature_invalid" })
    expect(cases.get("bundle_key_not_root_certified")?.expect).toEqual({ ok: false, code: "signature_invalid" })
    expect(cases.get("archive_sha256_mismatch")?.expect).toEqual({ ok: false, code: "record_mismatch", field: "archive_sha256" })
    expect(cases.get("record_member_bytes_changed")?.expect).toEqual({
      ok: false,
      code: "record_mismatch",
      field: "record_member_sha256",
      member: "records/cortexkit/starter-persona/1.0.0.json",
    })
    expect(cases.get("record_missing_status")?.expect).toEqual({ ok: false, code: "status_invalid", field: "status" })
    expect(cases.get("non_ustar_entry")?.expect).toEqual({ ok: false, code: "archive_invalid", rule: "magic", offset: 0 })
    expect(cases.get("path_escaping_entry")?.expect).toMatchObject({ ok: false, code: "archive_invalid", rule: "path_invalid", member: "../outside.txt" })
    expect(cases.get("path_escaping_entry")?.expect.offset).toEqual(expect.any(Number))
    expect(cases.get("extra_unlisted_file")?.expect).toMatchObject({ ok: false, code: "archive_invalid", rule: "member_unlisted", member: "unlisted.txt" })
    expect(cases.get("extra_unlisted_file")?.expect.offset).toEqual(expect.any(Number))
    for (const entry of vector.cases) expect(sha256Hex(Buffer.from(entry.bundle_archive_base64, "base64"))).toBe(entry.bundle_archive_sha256)
  })
})

describe("ck-catalog-v1 prerelease ranges", () => {
  test("rejects prereleases in multi-comparator sets", () => {
    for (const range of [
      "1.3.0-beta.1 >=1.0.0",
      "=1.3.0-beta.1 >=1.0.0",
      ">=1.0.0 1.3.0-beta.1",
      ">=1.0.0 =1.3.0-beta.1",
      "1.3.0-beta.1 =1.3.0-beta.1",
      "1.3.0-beta.1 >=1.0.0 || ^2",
      "^2 || =1.3.0-beta.1 >=1.0.0",
      "1.3.0-beta.1 || =1.3.0-beta.1 >=1.0.0",
    ]) {
      expect(parseRange(range)).toBeNull()
    }
  })

  test("keeps exact prereleases legal in singleton sets", () => {
    const version = parseVersion("1.3.0-beta.1")!
    for (const range of [
      "1.3.0-beta.1",
      "=1.3.0-beta.1",
      "1.3.0-beta.1 || ^2",
      "^2 || =1.3.0-beta.1",
      "1.3.0-beta.1 || =1.4.0-beta.2",
    ]) {
      const parsed = parseRange(range)
      expect(parsed).not.toBeNull()
      expect(satisfies(version, parsed!)).toBe(true)
    }
  })

  test("resolver refuses compound prerelease ranges before candidate matching", () => {
    const range = "1.3.0-beta.1 >=1.0.0"
    const prerelease = { version: "1.3.0-beta.1", kind: "persona" as const, status: "available" as const, requires: { capabilities: [], packages: {} } }
    expect(resolve({ "@x/head": [prerelease] }, { package: "@x/head", range }, [])).toEqual({
      ok: false,
      refusal: { code: "range_invalid", package: "@x/head", range, requested_by: "(root)" },
    })
    expect(resolve({
      "@x/head": [{ ...prerelease, version: "1.0.0", requires: { capabilities: [], packages: { "@x/a": range } } }],
      "@x/a": [prerelease],
    }, { package: "@x/head", range: "^1" }, [])).toEqual({
      ok: false,
      refusal: { code: "range_invalid", package: "@x/a", range, requested_by: "@x/head@1.0.0" },
    })
  })
})

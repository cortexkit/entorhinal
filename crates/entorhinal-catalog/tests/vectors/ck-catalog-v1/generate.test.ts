import { describe, expect, test } from "bun:test"
import { readFileSync } from "node:fs"
import { join } from "node:path"
import { driftedFiles, JSON_FILES, parseRange, parseStrictJson, parseVersion, resolve, satisfies, SKILL_PACKAGE, TEST_KEY_1, VECTOR_DIR, verifyRecord } from "./generate"

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

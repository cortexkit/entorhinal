import { describe, expect, test } from "bun:test"
import { readFileSync } from "node:fs"
import { join } from "node:path"
import { driftedFiles, JSON_FILES, VECTOR_DIR } from "./generate"

const committed = (name: string) => readFileSync(join(VECTOR_DIR, name), "utf8")

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

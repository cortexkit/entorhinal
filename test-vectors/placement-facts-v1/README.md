# Placement facts v1

`records.json` contains byte-exact replies from the real `placement.read` handler,
open to a reserved reader. The cases, in order, are: before activation (empty),
the first backfilled page with a next cursor, the final backfilled page, a new
registration with null workspace, an assignment, the unchanged read after a
same-workspace assignment (no new fact), and a project removal with null workspace.
Both nullable fact fields are present and field order is part of the contract.

The scratch fixture uses fixed commit clocks, author and shared entry ids; it
never reads an operator's store. Backfill and subsequent writes run the core
journal mutation path, and every recorded reply is served by the real handler.

## Reproduce and verify

From the repository root:

```sh
ENTO_WRITE_SERVED_VECTORS=1 cargo test --workspace --all-targets --locked -- served_vectors_placement_facts_v1
(cd test-vectors/placement-facts-v1 && shasum -a 256 records.json > SHA256SUMS)
cargo test --workspace --all-targets --locked -- served_vectors_placement_facts_v1
(cd test-vectors/placement-facts-v1 && shasum -a 256 -c SHA256SUMS)
```

The default test byte-compares the published file in CI; write mode is explicit.
All timestamps are milliseconds since the Unix epoch. `through` is an insertion
position, not a clock. Consumers deduplicate by `fact_id`, and restart at position
0 when a cursor's incarnation no longer matches the serving process.

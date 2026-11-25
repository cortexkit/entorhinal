# Prefrontal `agent-data/v1` samples

`replies.json` contains successful replies for all ten participant operations and a
typed `prepare` refusal. Each record names the destination of the call (`node`),
the `request` (`method` and `params`), and the `reply` (`result` or `error`, with
the transport envelope omitted). The replies come from Prefrontal's participant
handler serving `reserved:entorhinal` with two scratch stores, a fake Engram door,
and a fake receiver. The `2222…` cut is aborted; the `1111…` cut is transferred,
activated, and released. The prepare, export, and import replies for `1111…`
refer to the same stream. `prepare-busy` is captured while the origin is writing
an ask answer, before the cut is prepared.

`requests/` holds the copied Entorhinal driver captures from source revision
`e0d0ec8`, including the release form with
`holderBox`. The Rust test decodes the 14 participant requests per file with
`cortexkit-role-agent-data` 0.3.4 and checks that encoding preserves their
parameters, including the exact session refs. The two preceding registry calls
in each file are retained for provenance; they are not participant operations.
The copied requests use `opencode-session`; Prefrontal's own reply samples use
`prefrontal-agent`.

From the repository root, check the files with:

```sh
cargo nextest run --locked -p prefrontal-core-module --lib --bins -E 'test(/runtime_tests::agent_move::vectors::/)'
```

Set `UPDATE_PREFRONTAL_AGENT_DATA_VECTORS=1` on that command to regenerate
`replies.json` and `SHA256SUMS`. It leaves the copied requests untouched. These
tests check Prefrontal's served replies and the request decoder; Entorhinal
must separately check the pinned replies against its real driver before
Prefrontal advertises `agent-data/v1`.

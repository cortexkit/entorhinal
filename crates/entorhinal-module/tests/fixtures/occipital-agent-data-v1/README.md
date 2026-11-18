# Occipital agent-data/v1 samples

The five JSON files and `SHA256SUMS` are byte-for-byte copies of
`crates/occipital-module/test-vectors/agent-data-v1/` in Occipital at tag
`occipital-agent-data-v1` (tag object `62b4e49e6c071909b54856f77f09f9a08403fafa`,
commit `5d737bcadb830f09b74491e7d382fc9d2dff22ec`).

They cover source and receiver calls, release, an agent with no Occipital
rows, and refusals. Each exchange records the caller principal, request and
reply from Occipital's handler. The test replays the wire bodies against
Entorhinal's participant client; the recorded principal describes the
producer's test case, not a change to the driver's caller identity.

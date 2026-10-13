# Frozen generic-consent inputs

These bytes are copied from Cingulate `7a1c32c`,
`test-vectors/consent-requester-v2`. The `plexus-request.json` and
`worker-consent-request.json` inputs are byte-identical to
`cerebellum-request.json`; their differences are daemon identity admission,
not decoder input. The reference-bearing input is also retained independently.

`src/consent_decoder.rs` ports the generic `consent.request` acceptance rules
from `cingulate-decode` 0.4.0 at that revision. Its comments cite the original
`src/lib.rs` lines. It is test-only, not a production or path dependency.
The tests exercise the decoder on these inputs, malformed variants, and all
four rendered Entorhinal cards. This is not a port of permission ask decoding
or requester principal admission.

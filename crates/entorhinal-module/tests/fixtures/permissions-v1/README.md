# Permission provider vectors

The `provider-*.json` files are unchanged copies from Cingulate commit `7a1c32c`,
the `test-vectors/permissions-v1` corpus described in Cingulate's
`docs/contracts/permissions-v1.md`. Provider schemas
and admission are unchanged from the older consent contract; the newer contract
adds Cingulate aggregator refusals, served vectors and simulator list admission.
Those aggregator operations belong to Cingulate, not this provider.

Entorhinal tests compare revoke and refusal envelopes directly and adapt the
always-grant list row only for its agent identity and provider-specific scope,
subject and wording. Entorhinal grants are always agent-held, never expiring or
worker-held; revocation stops future writes rather than running work. Thus the
Cerebellum example vectors for timed expiry, workers and `stopping` are retained
as contract context, not claimed as Entorhinal behavior. Separate tests cover
holder/subject termination, in-place refresh, filtering and prompt-free revoke.

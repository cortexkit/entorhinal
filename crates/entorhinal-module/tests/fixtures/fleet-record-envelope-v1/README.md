# Fleet record envelope v1

`cases.json` pins the exact UTF-8 bytes of the AAD, plaintext, signing input,
and envelope for a sealed identity-log entry and a sealed residence value. There
are no trailing newlines inside these strings. Each negative case changes one
field of the residence envelope without re-signing it. The altered-key case is
read at its altered destination so it exercises the signature, not just the
destination check.

The samples were produced independently of Entorhinal with Python's sorted,
compact JSON serializer (all property names are ASCII and all numbers are small
integers) and libsodium. The Ed25519 seed is byte `51` repeated 32 times, and
`key_version` is the first eight SHA-256 bytes of the public key, in hex.
For each sample, XChaCha20-Poly1305 uses the envelope nonce, the key byte `31`
repeated 32 times, and the RFC 8785 canonical JSON (JCS) of `{aad,epoch,v}` as its associated data. Here
`aad` is unpadded base64url of `aad_utf8` and `v` is `ck-fleet-epoch-aad/v1`.
These are public synthetic keys and fixed nonces for test vectors only.

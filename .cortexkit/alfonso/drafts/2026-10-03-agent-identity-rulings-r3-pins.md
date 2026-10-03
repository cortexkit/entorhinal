These rulings settle round 3's needs_evidence question about the protocol pins (finding 13). They come from git, read directly on 2026-10-03.

R13 At HEAD `ff07aff8445f53f85b0f48f13c7a1fcada887bcb`, both files pin the 0.29 versions:
- `Cargo.toml` has `subc-protocol = "0.29.0"` and `subc-client-rs = "0.26.1"`;
- `Cargo.lock` resolves exactly `subc-protocol 0.29.0` and `subc-client-rs 0.26.1`, one copy of each.

Commit `192f220` (entorhinal 0.1.14) is an ancestor of HEAD and later than `3e06dbd37b62`, where the earlier evidence package was frozen. The pin moves landed after that freeze, in this order:
- `c355be4` (0.1.13) moved subc-protocol 0.28.1 → 0.29.0, subc-client-rs 0.25.2 → 0.26.0, subc-transport 0.9.1 → 0.10.0 and subc-control 0.27.1 → 0.28.0;
- `192f220` (0.1.14) moved subc-client-rs 0.26.0 → 0.26.1.

The package's record of 0.28.1 / 0.25.2 (ev-56, ev-57) is therefore stale, not a contradiction. "Start from 0.29.0 / 0.26.1 and change neither pin" holds at HEAD, and acceptance item 10's pin test asserts those two versions.

R14 This refire reads evidence at HEAD `ff07aff` (its integration_ref), so every file in the evidence package reflects the code the slices will be cut from. Since `3e06dbd37b62` the only source change is the session-liveness receiver in `crates/entorhinal-module/src/main.rs`: refusing older batches, the `livenessDroppedOlder` gauge, and a mirror that starts stale until a snapshot arrives. That code is outside this campaign's scope, and slices must leave it unchanged.

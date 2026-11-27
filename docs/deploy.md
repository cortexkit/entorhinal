# GitHub releases

The [Release workflow](../.github/workflows/release.yml) uses
[`script/release-build.sh`](../script/release-build.sh) to build `ck-entorhinal`
from a clean checkout. It publishes one ZIP and one SHA-256 sidecar for each of
`darwin-arm64`, `linux-x64`, `linux-arm64`, `windows-x64`, and `windows-arm64`, plus
`release-manifest.json` for `ck setup`. The ZIP holds only `ck-entorhinal` (or
`ck-entorhinal.exe` on Windows). Placement creates the `ck-projects`,
`ck-workspaces`, `ck-agents`, `ck-roles`, and `ck-packages` links from the
`cli_faces` list printed by `ck-entorhinal --manifest`.

On macOS, the workflow signs `ck-entorhinal` with Developer ID, the hardened
runtime, and the identifier `ck-entorhinal`. It needs no entitlements. The
workflow notarizes the ZIP before checking the packaged binary and recording
the ZIP checksum.

Every target is built and tested natively. For `darwin-arm64`, `linux-x64`,
and `windows-x64`, publishing requires green push CI on the tagged commit,
including all three Windows x64 shards. The release workflow runs the full
locked suite on `ubuntu-24.04-arm` and `windows-11-arm` before building the
respective `linux-arm64` and `windows-arm64` archives.

Set the `entorhinal-module` crate version on `release/<version>`, wait for push
CI to pass, and tag that exact commit `v<version>`. A tag with the new version
will not match the crate version on `main`. Publishing also requires
`ENTORHINAL_REQUIRES_CORE` to be set in the workflow to the confirmed minimum
public core version. The workflow uploads to a draft and publishes only after
downloading and comparing every asset.
A manual dispatch rehearses the build, signing, and verification without
writing to GitHub Releases, even if the core floor is still unset.

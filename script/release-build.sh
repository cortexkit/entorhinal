#!/usr/bin/env bash
# Build ck-entorhinal with its clean source revision stamped in the manifest.
set -euo pipefail

cd "$(dirname "$0")/.."

die() { printf 'release-build.sh: %s\n' "$*" >&2; exit 1; }
# shellcheck source=script/check-release-binaries.sh
. ./script/check-release-binaries.sh

revision=$(git rev-parse HEAD)
[[ "$revision" == "${RELEASE_REVISION:-$revision}" ]] || die "expected $RELEASE_REVISION, checked out $revision"

# Refuse a dirty tree rather than claim that its bytes were built from HEAD.
dirty=$(git status --porcelain --untracked-files=all)
if [[ -n "$dirty" ]]; then
  die "refusing to stamp a dirty tree: commit or stash first, or use a plain cargo build for a dev binary"
fi

version=${RELEASE_VERSION:-}
if [[ -z "$version" ]]; then
  version=$(cargo metadata --locked --no-deps --format-version 1 \
    | release_python -c 'import json,sys; print(next(p["version"] for p in json.load(sys.stdin)["packages"] if p["name"] == "entorhinal-module"))')
fi

echo "building at $revision"
# Entorhinal's build.rs reads Git directly; no external stamp override is needed.
CARGO_TARGET_DIR=target cargo build --workspace --locked --release --bin ck-entorhinal

after_rev=$(git rev-parse HEAD)
dirty=$(git status --porcelain --untracked-files=all)
if [[ "$after_rev" != "$revision" || -n "$dirty" ]]; then
  die "refusing to report provenance: checkout changed during the build (expected clean $revision; HEAD is $after_rev)"
fi

check_release_binaries target/release "$version" "$revision"

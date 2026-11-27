#!/usr/bin/env bash
# Sign before packaging; the release workflow checksums only the final ZIPs.
set -euo pipefail

script_dir=$(cd "$(dirname "$0")" && pwd)

source_dir="${1:?usage: $0 <source-dir> <asset-dir> <darwin-target>}"
asset_dir="${2:?asset directory is required}"
target="${3:?darwin target is required}"
case "$target" in
    darwin-arm64) ;;
    *) echo "unsupported macOS release target: $target" >&2; exit 1 ;;
esac

: "${APP_STORE_CONNECT_API_KEY_PATH:?App Store Connect API key path is required}"
: "${APP_STORE_CONNECT_API_KEY_ID:?App Store Connect API key ID is required}"
: "${APP_STORE_CONNECT_API_ISSUER_ID:?App Store Connect API issuer ID is required}"
[[ -r "$APP_STORE_CONNECT_API_KEY_PATH" ]] || { echo "App Store Connect API key is not readable" >&2; exit 1; }

# Apple Development can sign the file but cannot notarize it. Fail before
# signing if there isn't exactly one valid Developer ID Application identity.
identities=$(security find-identity -v -p codesigning)
identity=$(printf '%s\n' "$identities" | awk -F'"' '$2 ~ /^Developer ID Application:/ { print $2 }')
if [[ -z "$identity" || "$identity" == *$'\n'* ]]; then
    echo "release requires exactly one valid Developer ID Application signing identity" >&2
    exit 1
fi

binary="$source_dir/ck-entorhinal"
# Entorhinal needs no entitlements. Set the identifier explicitly so it
# remains stable regardless of the path used to sign or run the binary.
codesign --force --options runtime --timestamp --sign "$identity" --identifier ck-entorhinal "$binary"
codesign --verify --strict "$binary"
signature=$(codesign -dv "$binary" 2>&1)
identifier=$(printf '%s\n' "$signature" | sed -n 's/^Identifier=//p')
flags=$(printf '%s\n' "$signature" | sed -n 's/^CodeDirectory .*flags=\([^ ]*\).*/\1/p')
entitlements=$(codesign -d --entitlements - --xml "$binary" 2>/dev/null)
if [[ "$identifier" != ck-entorhinal || "$flags" != *\(*runtime*\) || -n "$entitlements" ]]; then
    echo "$binary must be signed as ck-entorhinal with hardened runtime and no entitlements" >&2
    exit 1
fi

python3 "$script_dir/assets.py" pack "$source_dir" "$asset_dir" "$target"
archive="$asset_dir/ck-entorhinal-$target.zip"
result=$(xcrun notarytool submit "$archive" \
    --key "$APP_STORE_CONNECT_API_KEY_PATH" \
    --key-id "$APP_STORE_CONNECT_API_KEY_ID" \
    --issuer "$APP_STORE_CONNECT_API_ISSUER_ID" \
    --output-format json --wait)
printf '%s\n' "$result"
if [[ $(printf '%s\n' "$result" | python3 -c 'import json, sys; print(json.load(sys.stdin)["status"])') != Accepted ]]; then
    echo "notarization was not accepted for $archive" >&2
    exit 1
fi
# ZIPs cannot carry stapled tickets. Gatekeeper can look up the ticket online.

#!/bin/sh
# Sourced by release-build.sh from the repository root.

release_python() {
  case "$(uname -s)" in
    MINGW*|MSYS*|CYGWIN*) python "$@" ;;
    *) python3 "$@" ;;
  esac
}

# check_release_binaries DIRECTORY VERSION REVISION
check_release_binaries() (
  platform=$(uname -s) || exit 1
  binary="$1/ck-entorhinal"
  case "$platform" in
    MINGW*|MSYS*|CYGWIN*) binary="$binary.exe" ;;
  esac

  release_python script/release/assets.py check-binary "$binary" "$2" "$3" || exit 1
  if [ "$platform" = Darwin ]; then
    # Mach-O debug-map entries (OSO records) point to object files on the build machine.
    symbols=$(nm -a "$binary") || exit 1
    if printf '%s\n' "$symbols" | grep ' OSO ' >/dev/null; then
      printf 'release-build.sh: %s contains debug-map entries\n' "$binary" >&2
      exit 1
    fi
    printf 'debug map: 0 entries in %s\n' "$binary"
  fi
)

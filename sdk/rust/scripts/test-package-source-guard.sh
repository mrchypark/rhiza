#!/bin/sh
# Verify prebuilt overrides cannot make a source-incomplete crate publishable.
set -eu
sdk=$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd)
scratch=$(mktemp -d "${TMPDIR:-/tmp}/rhiza-package-guard.XXXXXX")
trap 'rm -rf "$scratch"' EXIT HUP INT TERM
mkdir -p "$scratch/crate" "$scratch/lib"
: > "$scratch/lib/librhiza_ffi.a"
CARGO_PKG_VERSION=0.0.0 rustc --edition=2021 "$sdk/build.rs" -o "$scratch/build-script"
target=$(rustc -vV | sed -n 's/^host: //p')
case "$target" in
    *-apple-darwin) target_os=macos; target_env='' ;;
    *-unknown-linux-gnu) target_os=linux; target_env=gnu ;;
    *) echo "unsupported test host: $target" >&2; exit 1 ;;
esac
unset DOCS_RS
run_build() {
    env CARGO_MANIFEST_DIR="$scratch/crate" OUT_DIR="$scratch" \
        TARGET="$target" HOST="$target" CARGO_CFG_TARGET_OS="$target_os" \
        CARGO_CFG_TARGET_ENV="$target_env" RHIZA_NATIVE_LIB_DIR="$scratch/lib" \
        "$scratch/build-script" > "$scratch/out" 2> "$scratch/err"
}
if run_build; then
    echo 'FAIL incomplete crate accepted with prebuilt override' >&2; exit 1
fi
grep -F 'bundled native Go sources are missing' "$scratch/err" >/dev/null
mkdir -p "$scratch/crate/native/cmd/rhiza-ffi" "$scratch/crate/native/vendor"
: > "$scratch/crate/native/go.mod"
: > "$scratch/crate/native/cmd/rhiza-ffi/main.go"
: > "$scratch/crate/native/vendor/modules.txt"
run_build
grep -F 'cargo:rustc-link-lib=static=rhiza_ffi' "$scratch/out" >/dev/null
if DOCS_RS=1 sh "$sdk/scripts/stage-crate.sh" HEAD > "$scratch/out" 2> "$scratch/err"; then
    echo 'FAIL staging accepted DOCS_RS bypass' >&2; exit 1
fi
grep -F 'staging requires bundled native source verification' "$scratch/err" >/dev/null
if RHIZA_NATIVE_LIB_DIR="$scratch/lib" sh "$sdk/scripts/stage-crate.sh" HEAD > "$scratch/out" 2> "$scratch/err"; then
    echo 'FAIL staging accepted prebuilt override' >&2; exit 1
fi
grep -F 'staging requires bundled native source verification' "$scratch/err" >/dev/null
echo 'PASS incomplete package rejected and complete snapshot accepts prebuilt archive'

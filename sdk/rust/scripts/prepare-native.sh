#!/bin/sh
# Rebuild sdk/rust/native from the production Go sources required by cmd/rhiza-ffi,
# and vendor the Go module closure into it so a build of the packaged crate
# resolves every module from the crate instead of the network.
# Run from any directory; the output deliberately excludes Go tests and benchmarks.
set -eu

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
sdk_dir=$(CDPATH= cd -- "$script_dir/.." && pwd)
repo_dir=$(CDPATH= cd -- "$sdk_dir/../.." && pwd)
native_dir="$sdk_dir/native"
temporary_dir="$sdk_dir/.native.tmp.$$"

cleanup() { rm -rf "$temporary_dir"; }
trap cleanup EXIT HUP INT TERM

mkdir -p "$temporary_dir"
cp "$repo_dir/go.mod" "$repo_dir/go.sum" "$repo_dir/rhiza.go" "$repo_dir/replica.go" "$repo_dir/config_env.go" "$repo_dir/operator_handler.go" "$repo_dir/membership.go" "$temporary_dir/"

(
    cd "$repo_dir"
    export NATIVE_OUTPUT="$temporary_dir"
    find cmd/rhiza-ffi internal pkg -type f ! -name '*_test.go' -exec sh -c '
        for source do
            target="$NATIVE_OUTPUT/$source"
            mkdir -p "$(dirname "$target")"
            cp "$source" "$target"
        done
    ' sh {} +
)

rm -rf "$native_dir"
mv "$temporary_dir" "$native_dir"
trap - EXIT HUP INT TERM

hash_files() {
    if command -v sha256sum >/dev/null 2>&1; then
        sha256sum "$@"
    else
        shasum -a 256 "$@"
    fi
}

before=$(hash_files "$native_dir/go.mod" "$native_dir/go.sum")
(cd "$native_dir" && GOWORK=off go mod vendor)
after=$(hash_files "$native_dir/go.mod" "$native_dir/go.sum")
if [ "$before" != "$after" ]; then
    echo "FAIL go.mod or go.sum changed while vendoring; run go mod tidy on the module and retry" >&2
    exit 1
fi

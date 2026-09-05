#!/bin/sh
# Rebuild sdk/rust/native from the production Go sources required by cmd/rhiza-ffi.
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
cp "$repo_dir/go.mod" "$repo_dir/go.sum" "$repo_dir/rhiza.go" "$repo_dir/replica.go" "$temporary_dir/"

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

#!/bin/sh
# Download the prebuilt librhiza_ffi.a for a crate version, verify it against the
# checksum published beside it, and place it where RHIZA_NATIVE_LIB_DIR expects
# it. A build that uses the result needs no Go toolchain.
#
# Usage: fetch-native.sh <crate-version> [target] [destination]
#
# target defaults to the host triple; destination defaults to
# $RHIZA_NATIVE_LIB_DIR and then to ./.rhiza-native. The destination directory is
# printed on stdout, progress on stderr.
set -e

if [ $# -lt 1 ]; then
    echo "usage: $0 <crate-version> [target] [destination]" >&2
    exit 2
fi

version=$1
target=host
if [ $# -ge 2 ]; then
    target=$2
fi
destination=$RHIZA_NATIVE_LIB_DIR
if [ $# -ge 3 ]; then
    destination=$3
fi
if [ -z "$destination" ]; then
    destination=./.rhiza-native
fi

if [ "$target" = host ]; then
    target=$(rustc -vV | sed -n 's/^host: //p')
fi

case $target in
    aarch64-apple-darwin | x86_64-apple-darwin | x86_64-unknown-linux-gnu | aarch64-unknown-linux-gnu) ;;
    *)
        echo "FAIL unsupported target '$target'; use aarch64-apple-darwin, x86_64-apple-darwin, x86_64-unknown-linux-gnu or aarch64-unknown-linux-gnu" >&2
        exit 2
        ;;
esac

asset=librhiza_ffi-$version-$target.a
base=https://github.com/mrchypark/rhiza/releases/download/v$version
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT HUP INT TERM

echo "downloading $base/$asset" >&2
curl -fsSLo "$work/$asset" "$base/$asset"
curl -fsSLo "$work/$asset.sha256" "$base/$asset.sha256"

if command -v sha256sum >/dev/null 2>&1; then
    ( cd "$work" && sha256sum -c "$asset.sha256" >&2 )
else
    ( cd "$work" && shasum -a 256 -c "$asset.sha256" >&2 )
fi

mkdir -p "$destination"
mv "$work/$asset" "$destination/librhiza_ffi.a"
echo "placed $destination/librhiza_ffi.a" >&2

echo "$destination"


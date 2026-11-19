#!/bin/sh
# Verify that a release tag, the Rust SDK version, and crates.io agree.
#
# Usage: scripts/check-release-sync.sh [tag]
#
# Without an argument the tag pointing at HEAD is used. Exits non-zero when the
# tag is not annotated, when its version differs from sdk/rust/Cargo.toml, or
# when crates.io does not serve that crate version as a live release.
set -eu

crate=rhizadb
manifest=sdk/rust/Cargo.toml

repo_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$repo_root"

tag=${1:-}
if [ -z "$tag" ]; then
    tag=$(git describe --exact-match --tags 2>/dev/null) || {
        echo "FAIL no tag points at HEAD; pass the release tag explicitly" >&2
        exit 1
    }
fi

if [ "$(git cat-file -t "refs/tags/$tag" 2>/dev/null || true)" != tag ]; then
    echo "FAIL $tag is not an annotated tag; create it with: git tag -a $tag -m 'Rhiza $tag'" >&2
    exit 1
fi

tag_version=${tag#v}
manifest_version=$(sed -n 's/^version = "\(.*\)"/\1/p' "$manifest" | head -n 1)

if [ -z "$manifest_version" ]; then
    echo "FAIL could not read the package version from $manifest" >&2
    exit 1
fi

if [ "$tag_version" != "$manifest_version" ]; then
    echo "FAIL tag $tag is $tag_version but $manifest declares $manifest_version" >&2
    exit 1
fi

operator_refs=$(grep -R -l 'ghcr.io/mrchypark/rhiza-operator:v[0-9]' deploy/operator deploy/operator-managed || true)
if [ -z "$operator_refs" ]; then
    echo "FAIL no versioned rhiza-operator deployment refs found" >&2
    exit 1
fi
for file in $operator_refs; do
    versions=$(sed -n 's/.*ghcr\.io\/mrchypark\/rhiza-operator:v\([0-9][^"[:space:]]*\).*/\1/p' "$file")
    for version in $versions; do
        if [ "$version" != "$tag_version" ]; then
            echo "FAIL $file references rhiza-operator:v$version, expected v$tag_version" >&2
            exit 1
        fi
    done
done
managed_tag=$(sed -n 's/^[[:space:]]*newTag:[[:space:]]*v\(.*\)$/\1/p' deploy/operator-managed/kustomization.yaml | head -n 1)
if [ "$managed_tag" != "$tag_version" ]; then
    echo "FAIL deploy/operator-managed kustomization image tag is ${managed_tag:-missing}, expected $tag_version" >&2
    exit 1
fi

if ! body=$(curl -fsS -H 'User-Agent: rhiza-release-sync' "https://crates.io/api/v1/crates/$crate/$tag_version"); then
    echo "FAIL $crate $tag_version is not visible on crates.io; publish the crate before tagging it" >&2
    exit 1
fi

case $body in
    *'"num":"'"$tag_version"'"'*'"yanked":false'*) ;;
    *)
        echo "FAIL the crates.io record for $crate $tag_version is missing, mismatched, or yanked" >&2
        exit 1
        ;;
esac

echo "OK $tag == $crate $tag_version, published and not yanked; operator manifests synchronized"

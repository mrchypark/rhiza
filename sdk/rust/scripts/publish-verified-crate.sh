#!/bin/sh
# Publish only a freshly staged package matching full source verification.
# Usage: publish-verified-crate.sh COMMIT NEW_EVIDENCE_DIR
set -eu
script_dir=$(CDPATH='' cd -- "$(dirname -- "$0")" && pwd)
repo_dir=$(CDPATH='' cd -- "$script_dir/../../.." && pwd)
commit=$(git -C "$repo_dir" rev-parse --verify "${1:?commit required}^{commit}")
output=${2:?new evidence directory required}
[ ! -e "$output" ] || { echo 'FAIL evidence directory already exists' >&2; exit 1; }
[ "${DOCS_RS+x}" != x ] && [ "${RHIZA_NATIVE_LIB_DIR+x}" != x ] || {
    echo 'FAIL publication requires bundled native source verification' >&2; exit 1;
}
mkdir -p "$output"
output=$(CDPATH='' cd -- "$output" && pwd)
cleanup() {
    result=$?
    trap - EXIT HUP INT TERM
    if [ -f "$output/stage" ]; then
        stage=$(cat "$output/stage")
        case "$stage" in
            "${TMPDIR:-/tmp}"/rhiza-crate-stage.*)
                if ! rm -rf "$stage" || [ -e "$stage" ]; then
                    echo "FAIL retained owned stage: $stage" >&2
                    [ "$result" -ne 0 ] || result=1
                fi;;
            *) echo 'FAIL invalid stage path; retained for inspection' >&2; [ "$result" -ne 0 ] || result=1;;
        esac
    fi
    exit "$result"
}
trap cleanup EXIT
trap 'exit 129' HUP
trap 'exit 130' INT
trap 'exit 143' TERM

# This performs the complete staged-vs-package native inventory, bytes, mode,
# Git provenance and isolated source-build checks before publication.
sh "$script_dir/ci-package-artifact.sh" "$commit" "$output/verification"
RHIZA_CRATE_STATE_DIR="$output" sh "$script_dir/stage-crate.sh" "$commit" > "$output/stage-result" 2> "$output/stage.log"
stage=$(cat "$output/stage-result")
[ "$stage" = "$(cat "$output/stage")" ] || { echo 'FAIL stage identity mismatch' >&2; exit 1; }
version=$(sed -n 's/^version = "\(.*\)"/\1/p' "$stage/Cargo.toml" | head -n 1)
case "$version" in ''|*[!0-9.]*) echo 'FAIL invalid package version' >&2; exit 1;; esac
name="rhizadb-$version.crate"
verified="$output/verification/verified/$name"
staged="$stage/target/package/$name"
cmp "$verified" "$staged" || { echo 'FAIL staged package differs from verified artifact' >&2; exit 1; }

# Cargo uses its configured credential provider; never put a token in argv.
echo 'Publishing verified stage; registry publication cannot be rolled back by this script.' >&2
cargo publish --manifest-path "$stage/Cargo.toml" --locked
cmp "$verified" "$staged" || { echo 'FAIL Cargo repackaged different bytes; registry may already contain this version' >&2; exit 1; }
url="https://static.crates.io/crates/rhizadb/$name"
if ! curl --fail --silent --show-error --location --retry 8 --retry-delay 2 --retry-all-errors "$url" -o "$output/registry.crate"; then
    echo 'FAIL registry readback unavailable; publication may have succeeded; inspect before retrying' >&2; exit 1
fi
cmp "$verified" "$output/registry.crate" || {
    echo 'FAIL registry bytes differ from verified artifact; publication may have succeeded' >&2; exit 1;
}
echo 'PASS published registry bytes match complete verified native snapshot'

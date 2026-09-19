#!/bin/sh
# Stage the rhizadb crate outside the repository, from an exact commit.
#
# The stage is built from a detached worktree of one commit, so the package can
# only contain that commit's sources plus the sdk/rust/native tree generated from
# them. Cargo sees no repository inside the stage, so there is no --allow-dirty
# escape hatch and no way to publish arbitrary working-tree state.
#
# Usage: sdk/rust/scripts/stage-crate.sh [ref]
#
# Prints the stage directory on stdout. Progress, the crate path, and its sha256
# go to stderr.
set -eu

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
sdk_dir=$(CDPATH= cd -- "$script_dir/.." && pwd)
repo_dir=$(CDPATH= cd -- "$sdk_dir/../.." && pwd)

ref=${1:-HEAD}
commit=$(git -C "$repo_dir" rev-parse --verify "${ref}^{commit}")

worktree_dir=$(mktemp -d "${TMPDIR:-/tmp}/rhiza-crate-src.XXXXXX")
stage_dir=$(mktemp -d "${TMPDIR:-/tmp}/rhiza-crate-stage.XXXXXX")

trap 'git -C "$repo_dir" worktree remove --force "$worktree_dir" >/dev/null 2>&1 || true' EXIT HUP INT TERM

git -C "$repo_dir" worktree add --detach --quiet "$worktree_dir" "$commit"
if [ -n "$(git -C "$worktree_dir" status --porcelain --untracked-files=no)" ]; then
    echo "FAIL the worktree for $commit is not clean" >&2
    exit 1
fi

# Regenerate the bundled Go tree from this commit's sources, never from the
# caller's working tree.
sh "$worktree_dir/sdk/rust/scripts/prepare-native.sh"
tar -cf - --exclude=./target -C "$worktree_dir/sdk/rust" . | tar -xf - -C "$stage_dir"

version=$(sed -n 's/^version = "\(.*\)"/\1/p' "$stage_dir/Cargo.toml" | head -n 1)
cargo package --manifest-path "$stage_dir/Cargo.toml" --locked >&2

crate=$(ls "$stage_dir"/target/package/*.crate)
size=$(wc -c < "$crate" | tr -d ' ')
limit=10000000
if [ "$size" -gt "$limit" ]; then
    echo "FAIL the packaged crate is $size bytes, over the $limit byte crates.io limit" >&2
    exit 1
fi
if command -v shasum >/dev/null 2>&1; then
    hash=$(shasum -a 256 "$crate" | awk '{print $1}')
else
    hash=$(sha256sum "$crate" | awk '{print $1}')
fi

{
    echo "staged rhizadb $version from $ref ($commit)"
    echo "crate:  $crate"
    echo "size:   $size bytes of the $limit byte crates.io limit"
    echo "sha256: $hash"
} >&2

echo "$stage_dir"

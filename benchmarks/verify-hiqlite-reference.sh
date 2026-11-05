#!/usr/bin/env bash
set -euo pipefail

[[ $# -le 1 ]] || { echo "usage: $0 [REFERENCE]" >&2; exit 2; }
repo_root=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd -P)
reference=${1:-$repo_root/benchmarks/hiqlite-reference.json}
evidence=$repo_root/benchmarks/results/2026-11-06-hiqlite-v0.16.0/evidence

[[ -f $reference && -r $reference ]] || { echo "unreadable reference: $reference" >&2; exit 1; }
[[ -d $evidence && -r $evidence ]] || { echo "unreadable evidence directory: $evidence" >&2; exit 1; }
[[ $(sha256sum "$repo_root/benchmarks/hiqlite-one-peer.patch" | cut -d' ' -f1) == e056c066efc27a96a7c7372ede87bbd794fdc65c0f41fa4daf958b3f68c2b07e ]] || exit 1
jq -e '
  .version == "v0.16.0" and
  .commit == "fd45e758af9ab2ff976813a6a511772d676562f1" and
  .patch_sha256 == "e056c066efc27a96a7c7372ede87bbd794fdc65c0f41fa4daf958b3f68c2b07e" and
  .source_run == "https://github.com/mrchypark/rhiza/actions/runs/42081996466" and
  .source_head_sha == "92354d42de66d44dd4fb999156093867cf900b0c" and
  .runner_image == "ubuntu24-20261101.360.1" and
  .rust_version == "rustc 1.97.1 (8bab26f4f 2026-07-14)"
' "$reference" >/dev/null || exit 1
bash "$repo_root/benchmarks/verify-hiqlite-reference-artifact.sh" "$reference" "$evidence"

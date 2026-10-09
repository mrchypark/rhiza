#!/usr/bin/env bash
set -euo pipefail

reference=${1:-benchmarks/hiqlite-reference.json}
evidence=benchmarks/results/2026-09-26-hiqlite-v0.15.0/evidence

[[ $(sha256sum benchmarks/hiqlite-one-peer.patch | cut -d' ' -f1) == e056c066efc27a96a7c7372ede87bbd794fdc65c0f41fa4daf958b3f68c2b07e ]] || exit 1
jq -e '
  .version == "v0.15.0" and
  .commit == "cc0c64c9e2369ad9bd8ee5aea8e6825e3d481249" and
  .patch_sha256 == "e056c066efc27a96a7c7372ede87bbd794fdc65c0f41fa4daf958b3f68c2b07e" and
  .source_run == "https://github.com/mrchypark/rhiza/actions/runs/36226289455" and
  .source_head_sha == "2708a49b5ff35321bed67779b0f27c4f14f6e203" and
  .runner_image == "ubuntu24-20260920.314.1" and
  .rust_version == "rustc 1.97.1 (8bab26f4f 2026-07-14)"
' "$reference" >/dev/null || exit 1
bash benchmarks/verify-hiqlite-reference-artifact.sh "$reference" "$evidence"

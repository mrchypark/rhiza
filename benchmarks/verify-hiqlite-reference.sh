#!/usr/bin/env bash
set -euo pipefail

reference=${1:-benchmarks/hiqlite-reference.json}
evidence=benchmarks/results/2026-09-26-hiqlite-v0.15.0/evidence
scenarios=(interval_200_local immediate_local immediate_remote immediate_follower_stopped immediate_leader_stopped)

[[ $(sha256sum benchmarks/hiqlite-one-peer.patch | cut -d' ' -f1) == e056c066efc27a96a7c7372ede87bbd794fdc65c0f41fa4daf958b3f68c2b07e ]]
jq -e '
  .version == "v0.15.0" and
  .commit == "cc0c64c9e2369ad9bd8ee5aea8e6825e3d481249" and
  .patch_sha256 == "e056c066efc27a96a7c7372ede87bbd794fdc65c0f41fa4daf958b3f68c2b07e" and
  .source_run == "https://github.com/mrchypark/rhiza/actions/runs/36226289455" and
  .source_head_sha == "2708a49b5ff35321bed67779b0f27c4f14f6e203" and
  .runner_image == "ubuntu24-20260920.314.1" and
  .rust_version == "rustc 1.97.1 (8bab26f4f 2026-07-14)" and
  .requests == 100000 and .concurrency == 16 and
  (.ops_per_sec | keys) == (["interval_200_local", "immediate_local", "immediate_remote", "immediate_follower_stopped", "immediate_leader_stopped"] | sort) and
  (.evidence_sha256 | keys) == (["environment.txt", "interval_200_local.txt.gz", "immediate_local.txt.gz", "immediate_remote.txt.gz", "immediate_follower_stopped.txt.gz", "immediate_leader_stopped.txt.gz"] | sort) and
  all(.ops_per_sec[]; type == "number" and . > 0 and floor == .)
' "$reference" >/dev/null

for file in environment.txt "${scenarios[@]/%/.txt.gz}"; do
  expected=$(jq -r --arg file "$file" '.evidence_sha256[$file]' "$reference")
  [[ $(sha256sum "$evidence/$file" | cut -d' ' -f1) == "$expected" ]]
done
grep -Fx "runner_image=$(jq -r '.runner_image' "$reference")" "$evidence/environment.txt" >/dev/null
grep -Fx "rust=$(jq -r '.rust_version' "$reference")" "$evidence/environment.txt" >/dev/null
for scenario in "${scenarios[@]}"; do
  actual=$(gzip -cd "$evidence/$scenario.txt.gz" | awk '/single INSERTs/ && !found {getline; result=$4; found=1} END {print result}')
  [[ $actual =~ ^[0-9]+$ ]]
  [[ $actual == "$(jq -r --arg scenario "$scenario" '.ops_per_sec[$scenario]' "$reference")" ]]
done

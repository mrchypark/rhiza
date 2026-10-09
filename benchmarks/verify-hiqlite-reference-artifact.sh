#!/usr/bin/env bash
set -euo pipefail

[[ $# == 2 ]] || { echo "usage: $0 REFERENCE EVIDENCE_DIR" >&2; exit 2; }
repo_root=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd -P)
reference=$1
evidence=$2
scenarios=(interval_200_local immediate_local immediate_remote immediate_follower_stopped immediate_leader_stopped)

[[ -f $reference && -r $reference ]] || { echo "unreadable reference: $reference" >&2; exit 1; }
[[ -d $evidence && -r $evidence ]] || { echo "unreadable evidence directory: $evidence" >&2; exit 1; }
[[ -f $repo_root/benchmarks/hiqlite-one-peer.patch && -r $repo_root/benchmarks/hiqlite-one-peer.patch ]] || exit 1
jq -e '
  (.version | type == "string" and startswith("v")) and
  (.commit | type == "string" and test("^[0-9a-f]{40}$")) and
  (.patch_sha256 | type == "string" and test("^[0-9a-f]{64}$")) and
  (.measured_at | type == "string" and test("Z$")) and
  (.source_run | type == "string" and startswith("https://github.com/")) and
  (.source_head_sha | type == "string" and test("^[0-9a-f]{40}$")) and
  (.runner_image | type == "string" and length > 0) and
  (.rust_version | type == "string" and startswith("rustc ")) and
  .requests == 100000 and .concurrency == 16 and
  (.ops_per_sec | keys) == (["interval_200_local", "immediate_local", "immediate_remote", "immediate_follower_stopped", "immediate_leader_stopped"] | sort) and
  (.evidence_sha256 | keys) == (["environment.txt", "interval_200_local.txt.gz", "immediate_local.txt.gz", "immediate_remote.txt.gz", "immediate_follower_stopped.txt.gz", "immediate_leader_stopped.txt.gz"] | sort) and
  all(.ops_per_sec[]; type == "number" and . > 0 and floor == .) and
  all(.evidence_sha256[]; type == "string" and test("^[0-9a-f]{64}$"))
' "$reference" >/dev/null || exit 1
[[ $(sha256sum "$repo_root/benchmarks/hiqlite-one-peer.patch" | cut -d' ' -f1) == "$(jq -r '.patch_sha256' "$reference")" ]] || exit 1

for file in environment.txt "${scenarios[@]/%/.txt.gz}"; do
  [[ -f $evidence/$file && -r $evidence/$file ]] || exit 1
  expected=$(jq -r --arg file "$file" '.evidence_sha256[$file]' "$reference")
  [[ $(sha256sum "$evidence/$file" | cut -d' ' -f1) == "$expected" ]] || exit 1
done
grep -Fx "runner_image=$(jq -r '.runner_image' "$reference")" "$evidence/environment.txt" >/dev/null || exit 1
grep -Fx "rust=$(jq -r '.rust_version' "$reference")" "$evidence/environment.txt" >/dev/null || exit 1
for scenario in "${scenarios[@]}"; do
  actual=$(gzip -cd "$evidence/$scenario.txt.gz" | awk '/single INSERTs/ && !found {getline; result=$4; found=1} END {print result}') || exit 1
  [[ $actual =~ ^[0-9]+$ ]] || exit 1
  [[ $actual == "$(jq -r --arg scenario "$scenario" '.ops_per_sec[$scenario]' "$reference")" ]] || exit 1
done

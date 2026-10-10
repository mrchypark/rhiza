#!/usr/bin/env bash
set -euo pipefail

repo_root=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd -P)
reference=$repo_root/benchmarks/hiqlite-reference.json
evidence=$repo_root/benchmarks/results/2026-10-10-hiqlite-v0.15.2/evidence
fixture=$(mktemp -d "${TMPDIR:-/tmp}/rhiza-hiqlite-verifier.XXXXXX")
trap 'rm -rf -- "$fixture"' EXIT

(cd "${TMPDIR:-/tmp}" && bash "$repo_root/benchmarks/verify-hiqlite-reference.sh")

mkdir -p "$fixture/repo/benchmarks"
cp "$repo_root/benchmarks/verify-hiqlite-reference-artifact.sh" "$fixture/repo/benchmarks/"
cp "$repo_root/benchmarks/hiqlite-one-peer.patch" "$fixture/repo/benchmarks/"
printf '\ninvalid patch\n' >>"$fixture/repo/benchmarks/hiqlite-one-peer.patch"
if bash "$fixture/repo/benchmarks/verify-hiqlite-reference-artifact.sh" "$reference" "$evidence"; then
  echo "modified patch unexpectedly verified" >&2
  exit 1
fi

cp -R "$evidence" "$fixture/evidence"
printf 'tampered=true\n' >>"$fixture/evidence/environment.txt"
if bash "$repo_root/benchmarks/verify-hiqlite-reference-artifact.sh" "$reference" "$fixture/evidence"; then
  echo "modified evidence unexpectedly verified" >&2
  exit 1
fi

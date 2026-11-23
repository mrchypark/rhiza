#!/bin/sh
set -eu

actual=$(git rev-parse --verify 'HEAD^{commit}')
resolved=$(sh benchmarks/validate-stall-candidate.sh "$actual")
[ "$resolved" = "$actual" ] || exit 1
status=0
sh benchmarks/validate-stall-candidate.sh 0000000000000000000000000000000000000000 >/dev/null 2>&1 || status=$?
[ "$status" -eq 2 ] || exit 1
status=0
sh benchmarks/validate-stall-candidate.sh '' >/dev/null 2>&1 || status=$?
[ "$status" -eq 2 ] || exit 1

#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 2 ]]; then
	printf 'usage: %s CLIENT_TAGS ASSESSMENT\n' "$0" >&2
	exit 2
fi

client_tags=$1
assessment=$2
if [[ ! -s $client_tags || ! -s $assessment ]]; then
	printf 'incomplete: missing client tags or assessment artifact\n' >&2
	exit 1
fi

expected_tags=$(awk 'END { print NR + 0 }' "$client_tags")
grep -qx '# status=valid' "$assessment"
grep -qx "# valid=$expected_tags" "$assessment"
grep -qx '# incomplete=0' "$assessment"
grep -qx '# client_failures=0' "$assessment"
grep -qx '# duplicate_events=0' "$assessment"
grep -qx '# lock_shard_mismatches=0' "$assessment"
grep -qx '# lock_shard_out_of_range=0' "$assessment"
grep -qx '# lock_outcome_failures=0' "$assessment"
grep -qx '# enqueue_outcome_failures=0' "$assessment"
grep -qx '# order_invalid=0' "$assessment"
grep -qx '# pair_limit_exceeded=0' "$assessment"
grep -qx '# pre_enqueue_drops=0' "$assessment"

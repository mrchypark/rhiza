#!/usr/bin/env bash
set -uo pipefail

if [[ $# -ne 2 && $# -ne 4 ]]; then
	printf 'usage: %s ARTIFACT_DIR PHASE [AWK SORT]\n' "$0" >&2
	exit 2
fi

dir=$1
phase=$2
awk_bin=${3:-awk}
sort_bin=${4:-sort}
script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
prefix=$dir/$phase-pre-enqueue

shopt -s nullglob
inputs=("$dir"/"$phase"-node-*-trace.txt)
if [[ ${#inputs[@]} -eq 0 ]]; then
	: > "$prefix-awk.stderr"
	: > "$prefix-sort.stderr"
	printf '%s\n' 2 > "$prefix-awk.exit"
	printf '%s\n' 0 > "$prefix-sort.exit"
	printf 'missing trace inputs for %s\n' "$phase" >&2
	exit 2
fi

set +e
"$awk_bin" -v client_file="$dir/$phase-client-tags.txt" \
	-f "$script_dir/assess-preenqueue-latency.awk" "${inputs[@]}" \
	> "$prefix-unsorted.txt" 2> "$prefix-awk.stderr"
awk_rc=$?
"$sort_bin" -n -k1,1 "$prefix-unsorted.txt" \
	> "$prefix-latency.txt" 2> "$prefix-sort.stderr"
sort_rc=$?
set -e

printf '%s\n' "$awk_rc" > "$prefix-awk.exit"
printf '%s\n' "$sort_rc" > "$prefix-sort.exit"
if [[ $awk_rc -ne 0 ]]; then exit "$awk_rc"; fi
if [[ $sort_rc -ne 0 ]]; then exit "$sort_rc"; fi

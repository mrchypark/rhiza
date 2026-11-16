#!/usr/bin/env bash
set -uo pipefail

if [[ $# -ne 3 ]]; then
	printf 'usage: %s OUTPUT AWK SORT\n' "$0" >&2
	exit 2
fi

output=$1
awk_bin=$2
sort_bin=$3
temporary=$output.tmp

awk_path=$(command -v "$awk_bin" 2>/dev/null || printf '%s' "$awk_bin")
sort_path=$(command -v "$sort_bin" 2>/dev/null || printf '%s' "$sort_bin")
awk_w_version=$("$awk_bin" -W version 2>&1)
awk_w_exit=$?
awk_version=$("$awk_bin" --version 2>&1)
awk_exit=$?
sort_version=$("$sort_bin" --version 2>&1)
sort_exit=$?

{
	printf 'awk_command=%s\n' "$awk_bin"
	printf 'awk_path=%s\n' "$awk_path"
	printf 'awk_W_version_exit=%s\n' "$awk_w_exit"
	printf '%s\n' "$awk_w_version"
	printf 'awk_version_exit=%s\n' "$awk_exit"
	printf '%s\n' "$awk_version"
	printf 'sort_command=%s\n' "$sort_bin"
	printf 'sort_path=%s\n' "$sort_path"
	printf 'sort_version_exit=%s\n' "$sort_exit"
	printf '%s\n' "$sort_version"
} > "$temporary"
mv "$temporary" "$output"

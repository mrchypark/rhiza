#!/bin/sh
# The runner must exercise the real client against a fresh three-node fixture.
set -eu

if [ "$#" -ne 4 ]; then
	printf 'usage: %s RUNNER CLIENT_BINARY SERVER_BINARY OUTPUT_DIR\n' "$0" >&2
	exit 2
fi
runner=$1
client=$2
server=$3
out=$4
samples=${RHIZA_PAIRED_SAMPLES:-5}
case "$samples" in ''|*[!0-9]*) echo 'invalid sample count' >&2; exit 2 ;; esac
if [ "$samples" -lt 5 ] || [ "$samples" -gt 100 ]; then
	echo 'sample count must be between 5 and 100' >&2
	exit 2
fi
for file in "$runner" "$client" "$server"; do
	if [ ! -f "$file" ] || [ ! -x "$file" ]; then
		printf 'not an executable file: %s\n' "$file" >&2
		exit 2
	fi
done
if [ -e "$out" ]; then
	echo 'output directory already exists' >&2
	exit 2
fi
mkdir -p "$out"
hash() { sha256sum "$1" | awk '{print $1}'; }
runner_hash=$(hash "$runner")
client_hash=$(hash "$client")
server_hash=$(hash "$server")
{
	printf 'revision=%s\n' "$(git rev-parse HEAD)"
	printf 'runner_sha256=%s\nclient_sha256=%s\nserver_sha256=%s\n' "$runner_hash" "$client_hash" "$server_hash"
	printf 'samples_per_mode=%s\n' "$samples"
	go version
	uname -a
} > "$out/environment.txt"

i=1
while [ "$i" -le "$samples" ]; do
	if [ $((i % 2)) -eq 1 ]; then modes='disabled enabled'; else modes='enabled disabled'; fi
	for mode in $modes; do
		for scenario in healthy slow-ingress dead-ingress; do
			dir="$out/$(printf '%02d' "$i")-$mode-$scenario"
			mkdir "$dir"
			# A successful runner exit means its own scenario oracle passed, not
			# that a disabled failover request was expected to succeed.
			status=0
			"$runner" "$mode" "$scenario" "$client" "$server" "$dir" > "$dir/runner.log" 2>&1 || status=$?
			printf '%s\n' "$status" > "$dir/runner.exit"
			if [ "$status" -ne 0 ]; then
				printf 'runner failed: sample=%s mode=%s scenario=%s exit=%s\n' "$i" "$mode" "$scenario" "$status" >&2
				exit 1
			fi
			if [ "$(hash "$runner")" != "$runner_hash" ] || [ "$(hash "$client")" != "$client_hash" ] || [ "$(hash "$server")" != "$server_hash" ]; then
				echo 'runner or binary changed during measurement' >&2
				exit 1
			fi
			if ! jq -e --arg mode "$mode" --arg scenario "$scenario" '
				.mode == $mode and .scenario == $scenario and
				(.successes | type == "number" and . >= 0) and
				(.errors | type == "number" and . >= 0) and
				(.p99_ms | type == "number" and . >= 0) and
				(.throughput | type == "number" and . >= 0) and
				.cleanup_verified == true and .correctness_verified == true and
				($scenario == "healthy" or .fault_verified == true)
			' "$dir/result.json" >/dev/null; then
				printf 'missing or invalid qualification: %s\n' "$dir/result.json" >&2
				exit 1
			fi
		done
	done
	i=$((i + 1))
done

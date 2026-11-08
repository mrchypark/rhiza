#!/usr/bin/env bash

set -euo pipefail

root=${1:-}
if [[ $# != 1 || $(uname -s) != Linux || ( $root != /tmp && $root != /dev/shm ) ]] ||
	[[ $(cd -P -- "$root" && pwd -P) != "$root" || ! -w $root ]]; then
	printf 'disk pressure requires a writable, canonical Linux /tmp or /dev/shm\n' >&2
	exit 2
fi

scratch=
cleanup() {
	local status=$?
	trap - EXIT
	if [[ -n $scratch ]] && ! { rm -f -- "$scratch/write.bin" && rmdir -- "$scratch"; }; then
		printf 'could not remove owned disk-pressure directory: %s\n' "$scratch" >&2
		status=1
	fi
	exit "$status"
}
trap cleanup EXIT
trap 'exit 0' TERM INT
scratch=$(mktemp -d "$root/rhiza-async-pressure.XXXXXX")

# Rewrite one fixed-size file; only the destination filesystem differs between arms.
deadline=$((SECONDS + 120))
while (( SECONDS < deadline )); do
	begin=$(date +%s%N)
	dd if=/dev/zero of="$scratch/write.bin" bs=256K count=1 conv=fdatasync,notrunc status=none
	end=$(date +%s%N)
	if [[ ! $begin =~ ^[0-9]{19}$ || ! $end =~ ^[0-9]{19}$ || $end < $begin ]]; then
		printf 'invalid disk-pressure timestamps\n' >&2
		exit 1
	fi
	printf 'DISK_PRESSURE_WRITE begin_ns=%s end_ns=%s\n' "$begin" "$end"
	sleep 0.01
done

printf 'disk-pressure writer was not stopped before its 120s limit\n' >&2
exit 1

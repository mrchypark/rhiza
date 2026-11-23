#!/bin/sh
set -eu
root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT HUP INT TERM

printf '#!/bin/sh\nexit 0\n' > "$tmp/binary"
chmod +x "$tmp/binary"
printf '%s\n' '#!/bin/sh' 'printf "%s %s\n" "$1" "$2" >> "$TRACE"' 'printf "{\"mode\":\"%s\",\"scenario\":\"%s\",\"successes\":1,\"errors\":0,\"p99_ms\":1,\"throughput\":1,\"cleanup_verified\":true,\"correctness_verified\":true,\"fault_verified\":true}\n" "$1" "$2" > "$5/result.json"' > "$tmp/runner"
chmod +x "$tmp/runner"
export TRACE="$tmp/order"
sh "$root/benchmarks/run-paired-service-reads.sh" "$tmp/runner" "$tmp/binary" "$tmp/binary" "$tmp/out"
[ "$(wc -l < "$tmp/order" | tr -d ' ')" = 30 ]
[ "$(sed -n '1p;7p' "$tmp/order")" = "$(printf 'disabled healthy\nenabled healthy')" ]
[ "$(find "$tmp/out" -name runner.exit | wc -l | tr -d ' ')" = 30 ]
if sh "$root/benchmarks/run-paired-service-reads.sh" "$tmp/runner" "$tmp/binary" "$tmp/binary" "$tmp/out"; then
	echo 'existing output should fail' >&2; exit 1
fi
printf '#!/bin/sh\nexit 42\n' > "$tmp/failing"
chmod +x "$tmp/failing"
if sh "$root/benchmarks/run-paired-service-reads.sh" "$tmp/failing" "$tmp/binary" "$tmp/binary" "$tmp/failed"; then
	echo 'runner failure should fail' >&2; exit 1
fi
[ "$(cat "$tmp/failed/01-disabled-healthy/runner.exit")" = 42 ]
printf '#!/bin/sh\nprintf "{}\\n" > "$5/result.json"\n' > "$tmp/invalid"
chmod +x "$tmp/invalid"
if sh "$root/benchmarks/run-paired-service-reads.sh" "$tmp/invalid" "$tmp/binary" "$tmp/binary" "$tmp/invalid-out"; then
	echo 'invalid result should fail' >&2; exit 1
fi

#!/usr/bin/env bash
# Execute the actual cleanup functions with an owned mock proc tree/Docker.
set -euo pipefail
script_dir=$(cd "$(dirname "$0")" && pwd)
test_root=$(mktemp -d "${TMPDIR:-/tmp}/rhiza-ack-cleanup-test.XXXXXX")
trap 'rm -rf "$test_root"' EXIT
extract() {
	awk -v name="$1" '$0==name"() {" {copy=1} copy {print} copy && $0=="}" {exit}' "$script_dir/run-server-reference.sh"
}
eval "$(extract proc_start)"
eval "$(extract diagnostic_cleanup)"
eval "$(extract cleanup)"
proc_root="$test_root/proc"
mkdir "$proc_root"
kill() {
	[[ ${*: -1} == 777 ]]
	printf '%s\n' "$*" >> "$evidence/kills"
	rm -rf "${proc_root:?}/777"
}
wait() { return 0; }
sleep() { :; }
docker() {
	if [[ $1 == ps ]]; then
		[[ ${docker_gap:-0} == 0 ]] || return 31
		printf '%s' "$mock_container"
	elif [[ $1 == rm ]]; then
		printf '%s\n' "$3" >> "$evidence/removes"
		mock_container=''
	else return 32; fi
}
diagnostic_ports() { : > "$1"; [[ ${ports_gap:-0} == 0 ]]; }
make_case() {
	run_dir="$test_root/rhiza-server-bench.$1"
	# shellcheck disable=SC2034 # Consumed by the actual extracted cleanup.
	output_file="$test_root/$1.json"
	evidence="$test_root/$1-evidence"
	mkdir -p "$run_dir/collector" "$evidence"
	container_id=$(printf '%064d' 1)
	mock_container=$container_id
	docker_gap=0
	ports_gap=0
}
make_pid() {
	mkdir -p "$proc_root/777"
	{
		printf '777 (owned) S'
		for _ in {1..18}; do printf ' 0'; done
		printf ' %s\n' "$1"
	} > "$proc_root/777/stat"
	printf '10\n' > "$evidence/777.start"
	printf '777\n' > "$run_dir/node-0.pid"
}
make_case success
make_pid 10
diagnostic_cleanup
grep -qx '777' "$evidence/kills"
grep -qx "$container_id" "$evidence/removes"
[[ ! -d $proc_root/777 ]]

make_case reused-pid
make_pid 11
if diagnostic_cleanup; then echo 'accepted reused PID' >&2; exit 1; fi
[[ ! -f $evidence/kills && -d $proc_root/777 ]]
rm -rf "$proc_root/777"

make_case docker-gap
docker_gap=1
if diagnostic_cleanup; then echo 'accepted Docker inventory gap' >&2; exit 1; fi
[[ ! -f $evidence/removes ]]

make_case endpoint-gap
ports_gap=1
if diagnostic_cleanup; then echo 'accepted occupied endpoint' >&2; exit 1; fi

make_case unowned-container
container_id=''
diagnostic_cleanup
[[ -n $mock_container && ! -f $evidence/removes ]]

make_case child-error
# shellcheck disable=SC2034 # Consumed by the actual extracted cleanup function.
diagnostic=1
ports_gap=1
if (trap cleanup EXIT; exit 17); then actual=0; else actual=$?; fi
[[ $actual == 17 ]]
grep -qx original_exit=17 "$evidence/cleanup.txt"
grep -qx cleanup_failed=1 "$evidence/cleanup.txt"
[[ -d $run_dir ]]

make_case successful-child-cleanup-gap
ports_gap=1
if (trap cleanup EXIT; exit 0); then actual=0; else actual=$?; fi
[[ $actual == 1 ]]
grep -qx original_exit=0 "$evidence/cleanup.txt"
grep -qx cleanup_failed=1 "$evidence/cleanup.txt"
echo 'PASS owned PID/container cleanup, reuse/metadata/endpoint negatives and original exit preservation (mock only)'

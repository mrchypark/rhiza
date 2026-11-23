#!/usr/bin/env bash
set -euo pipefail

here=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
scratch=$(mktemp -d "${TMPDIR:-/tmp}/rhiza-ingress-workflow-test.XXXXXX")
status=1
cleanup() {
	if [[ $status == 0 ]]; then rm -rf -- "$scratch"; else printf 'preserved workflow test evidence: %s\n' "$scratch" >&2; fi
}
trap cleanup EXIT
mkdir -p "$scratch/bin"

# Exercise the actual workflow shell with GitHub Actions' inherited bash -e.
awk '
  /^      - name: Measure healthy, peer failure, ingress failure and ingress reroute$/ { step=1; next }
  step && /^        run: \|$/ { body=1; next }
  body && /^      - name:/ { exit }
  body { sub(/^          /, ""); print }
' "$here/.github/workflows/phase-diagnostics.yml" > "$scratch/workflow.sh"
[[ -s "$scratch/workflow.sh" ]]

cat > "$scratch/bin/bash" <<'MOCK'
#!/bin/bash
set -eu
case "$3" in
  */ingress-no-bypass.json)
    printf '{"qualification":"expected_ingress_failure","errors":2,"fault_window":{"overlap":true}}\n' > "$3"
    exit 1 ;;
  */slow-no-bypass.json)
    if [[ ${FAIL_SLOW:-0} == 1 ]]; then
      printf '{"qualification":"expected_ingress_failure","successes":2,"errors":2,"error_kinds":{"transport":1,"http_0":1,"receipt":1},"slow_proxy_committed":1,"slow_proxy_timeout_witness":1}\n' > "$3"
    else
      printf '{"qualification":"expected_ingress_failure","successes":3,"errors":1,"error_kinds":{"transport":1,"http_0":1},"slow_proxy_committed":1,"slow_proxy_timeout_witness":1}\n' > "$3"
    fi
    exit 1 ;;
  */peer-fault.json)
    if [[ ${FAIL_PEER:-0} == 1 ]]; then exit 42; fi ;;
esac
printf '{}\n' > "$3"
MOCK
cat > "$scratch/bin/docker" <<'MOCK'
#!/bin/bash
exit 0
MOCK
chmod +x "$scratch/bin/bash" "$scratch/bin/docker"

for mode in expected unexpected unexpected-slow; do
	mkdir -p "$scratch/$mode"
	run_status=0
	PATH="$scratch/bin:$PATH" EVIDENCE_DIR="$scratch/$mode/evidence" TMPDIR="$scratch/$mode/temp" \
	  GITHUB_WORKSPACE="$here" RHIZA_SERVER_BENCH_REQUESTS=4 RHIZA_SERVER_BENCH_CONCURRENCY=2 \
	  FAIL_PEER=$([[ $mode == unexpected ]] && printf 1 || printf 0) \
	  FAIL_SLOW=$([[ $mode == unexpected-slow ]] && printf 1 || printf 0) \
	  /bin/bash -e -o pipefail "$scratch/workflow.sh" || run_status=$?
	[[ $(<"$scratch/$mode/evidence/ingress-no-bypass.exit") == 1 ]]
	[[ $(<"$scratch/$mode/evidence/ingress-reroute.exit") == 0 ]]
	[[ $(<"$scratch/$mode/evidence/slow-no-bypass.exit") == 1 ]]
	[[ $(<"$scratch/$mode/evidence/slow-reroute.exit") == 0 ]]
	if [[ $mode == expected ]]; then
		[[ $run_status == 0 ]]
	elif [[ $mode == unexpected ]]; then
		[[ $run_status == 1 && $(<"$scratch/$mode/evidence/peer-fault.exit") == 42 ]]
	else
		[[ $run_status == 1 ]]
	fi
done
status=0
printf 'ingress workflow expected and unexpected failures PASS\n'

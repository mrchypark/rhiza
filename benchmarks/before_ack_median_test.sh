#!/usr/bin/env bash
# shellcheck disable=SC2016
set -euo pipefail

here=$(cd "$(dirname "$0")" && pwd)
workflow="$here/../.github/workflows/performance.yml"
scratch=$(mktemp -d "${TMPDIR:-/tmp}/rhiza-before-ack-median-test.XXXXXX")
trap 'rm -rf -- "$scratch"' EXIT
sample_dir="$scratch/rhiza-performance"
mkdir -p "$sample_dir"

median_source=$(awk '
  /^          # BEGIN before_ack_median$/ { copy = 1; next }
  /^          # END before_ack_median$/ { exit }
  copy { sub(/^          /, ""); print }
' "$workflow")
[[ -n $median_source ]]
eval "$median_source"

assert_median() {
	local label=$1 got=$2 want=$3
	if jq -en --argjson got "$got" --argjson want "$want" \
		'($got - $want) as $delta | (if $delta < 0 then -$delta else $delta end) <= 1e-9' >/dev/null; then
		return
	fi
	printf 'median mismatch: %s got=%s want=%s\n' "$label" "$got" "$want" >&2
	exit 1
}

write_sample() {
	local file=$1 p99=$2 ops=$3 http=$4 put=$5
	jq -n --argjson p99 "$p99" --argjson ops "$ops" --argjson http "$http" --argjson put "$put" \
		'{result:{p99_ms:$p99,success_ops_per_sec:$ops},object_store_delta:{http_requests:$http,http_put_requests:$put}}' > "$file"
}

for sample in 1 2 3; do
	case $sample in
	1) p99=40.55908; ops=496.75346085173044; http=10; put=4 ;;
	2) p99=40.648028; ops=491.0118985518902; http=30; put=12 ;;
	3) p99=39.709154; ops=495.86748719638945; http=20; put=8 ;;
	esac
	write_sample "$sample_dir/rhiza-server-before-ack-base-$sample.json" "$p99" "$ops" "$http" "$put"
done
for sample in 1 2 3; do
	case $sample in
	1) p99=36.848403; ops=509.73993743848285; http=11; put=5 ;;
	2) p99=39.087322; ops=496.0701126995858; http=21; put=9 ;;
	3) p99=38.231271; ops=501.74687930080256; http=31; put=13 ;;
	esac
	write_sample "$sample_dir/rhiza-server-before-ack-candidate-$sample.json" "$p99" "$ops" "$http" "$put"
done

base_files=(
	"$sample_dir/rhiza-server-before-ack-base-1.json"
	"$sample_dir/rhiza-server-before-ack-base-2.json"
	"$sample_dir/rhiza-server-before-ack-base-3.json"
)
candidate_files=(
	"$sample_dir/rhiza-server-before-ack-candidate-1.json"
	"$sample_dir/rhiza-server-before-ack-candidate-2.json"
	"$sample_dir/rhiza-server-before-ack-candidate-3.json"
)

assert_median base_p99 "$(before_ack_median result.p99_ms "${base_files[@]}")" 40.55908
assert_median candidate_p99 "$(before_ack_median result.p99_ms "${candidate_files[@]}")" 38.231271
assert_median base_ops "$(before_ack_median result.success_ops_per_sec "${base_files[@]}")" 495.86748719638945
assert_median candidate_ops "$(before_ack_median result.success_ops_per_sec "${candidate_files[@]}")" 501.74687930080256

assert_workflow_line() {
	if ! grep -Fqx -- "$1" "$workflow"; then
		printf 'workflow wiring mismatch: %s\n' "$1" >&2
		exit 1
	fi
}

for mode in base candidate; do
	for sample in 1 2 3; do
		assert_workflow_line "            \"\$RUNNER_TEMP/rhiza-performance/rhiza-server-before-ack-$mode-$sample.json\""
	done
done
assert_workflow_line '          before_ack_base_ops=$(before_ack_median result.success_ops_per_sec "${before_ack_base_files[@]}")'
assert_workflow_line '          before_ack_base_p99=$(before_ack_median result.p99_ms "${before_ack_base_files[@]}")'
assert_workflow_line '          before_ack_base_http=$(before_ack_median object_store_delta.http_requests "${before_ack_base_files[@]}")'
assert_workflow_line '          before_ack_candidate_ops=$(before_ack_median result.success_ops_per_sec "${before_ack_candidate_files[@]}")'
assert_workflow_line '          before_ack_candidate_p99=$(before_ack_median result.p99_ms "${before_ack_candidate_files[@]}")'
assert_workflow_line '          before_ack_candidate_http=$(before_ack_median object_store_delta.http_requests "${before_ack_candidate_files[@]}")'
assert_workflow_line '          before_ack_candidate_put=$(before_ack_median object_store_delta.http_put_requests "${before_ack_candidate_files[@]}")'
assert_workflow_line '          bash benchmarks/before_ack_median_test.sh'

call_count=$(grep -Fc -- '$(before_ack_median ' "$workflow")
[[ $call_count -eq 7 ]]
if grep -Fq 'rhiza-server-before-ack-base-*.json' "$workflow" || grep -Fq 'rhiza-server-before-ack-candidate-*.json' "$workflow"; then
	printf 'before-ack median still uses a wildcard input\n' >&2
	exit 1
fi

# Unrelated matching-suffix files must not enter explicit sample selection.
printf '{"p99_ms":null}\n' > "$sample_dir/rhiza-server-before-ack-base-1-client-result.json"
assert_median base_p99_with_client_result "$(before_ack_median result.p99_ms "${base_files[@]}")" 40.55908

if before_ack_median result.p99_ms "${base_files[@]:0:2}" >/dev/null 2>&1; then
	printf 'missing sample count accepted\n' >&2
	exit 1
fi
if before_ack_median result.p99_ms "$sample_dir/missing.json" "${base_files[@]:1}" >/dev/null 2>&1; then
	printf 'missing sample file accepted\n' >&2
	exit 1
fi
if before_ack_median result.p99_ms "${base_files[0]}" "${base_files[0]}" "${base_files[1]}" >/dev/null 2>&1; then
	printf 'duplicate sample file accepted\n' >&2
	exit 1
fi

jq '.result.p99_ms = null' "${base_files[1]}" > "$scratch/null.json"
if before_ack_median result.p99_ms "${base_files[0]}" "$scratch/null.json" "${base_files[2]}" >/dev/null 2>&1; then
	printf 'null sample field accepted\n' >&2
	exit 1
fi

for nonfinite in NaN Infinity 1e999; do
	printf '{"result":{"p99_ms":%s,"success_ops_per_sec":1},"object_store_delta":{"http_requests":1,"http_put_requests":1}}\n' "$nonfinite" > "$scratch/nonfinite.json"
	if before_ack_median result.p99_ms "${base_files[0]}" "$scratch/nonfinite.json" "${base_files[2]}" >/dev/null 2>&1; then
		printf 'non-finite sample field accepted: %s\n' "$nonfinite" >&2
		exit 1
	fi
done

printf 'PASS before-ack median regression\n'

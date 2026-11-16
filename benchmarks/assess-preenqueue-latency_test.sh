#!/usr/bin/env bash
set -euo pipefail

here=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
assess="$here/../scripts/ci/assess-preenqueue-latency.awk"
verify_artifacts="$here/../scripts/ci/verify-sampled-diagnostic-artifacts.sh"
workflow="$here/../.github/workflows/performance.yml"
scratch=$(mktemp -d "${TMPDIR:-/tmp}/rhiza-preenqueue-test.XXXXXX")
trap 'rm -rf -- "$scratch"' EXIT

printf '16\t1\t1000\t40000\t100\t39000\t39000\ttrue\n32\t2\t1500\t40000\t1600\t38500\t38000\ttrue\n' > "$scratch/client.tsv"

emit_events() {
	local tag=$1 base=$2 slot=$3 shard=$4 wait_end=$5 hold_end=$6 proposal=$7
	printf '%s\n' \
		"rhiza_latency stage=mutation_admission batch=0 slot=0 count=0 peer=0000000000000000 outcome=ok start_unix_ns=$((base - 20)) end_unix_ns=$base duration_ns=20 request=0 attempt=0 tag=$tag" \
		"rhiza_latency stage=pre_enqueue_clone batch=0 slot=0 count=0 peer=0000000000000000 outcome=ok start_unix_ns=$base end_unix_ns=$((base + 10)) duration_ns=10 request=0 attempt=0 tag=$tag" \
		"rhiza_latency stage=pre_enqueue_validate batch=0 slot=0 count=0 peer=0000000000000000 outcome=ok start_unix_ns=$((base + 10)) end_unix_ns=$((base + 20)) duration_ns=10 request=0 attempt=0 tag=$tag" \
		"rhiza_latency stage=pre_enqueue_fingerprint batch=0 slot=0 count=0 peer=0000000000000000 outcome=ok start_unix_ns=$((base + 20)) end_unix_ns=$((base + 30)) duration_ns=10 request=0 attempt=0 tag=$tag" \
		"rhiza_latency stage=pre_enqueue_encode batch=0 slot=0 count=0 peer=0000000000000000 outcome=ok start_unix_ns=$((base + 30)) end_unix_ns=$((base + 40)) duration_ns=10 request=0 attempt=0 tag=$tag" \
		"rhiza_latency stage=request_lock_wait batch=0 slot=0 count=$shard peer=0000000000000000 outcome=ok start_unix_ns=$((base + 40)) end_unix_ns=$wait_end duration_ns=$((wait_end - base - 40)) request=0 attempt=0 tag=$tag" \
		"rhiza_latency stage=request_lock_hold batch=0 slot=0 count=$shard peer=0000000000000000 outcome=ok start_unix_ns=$wait_end end_unix_ns=$hold_end duration_ns=$((hold_end - wait_end)) request=0 attempt=0 tag=$tag" \
		"rhiza_latency stage=materializer_receipt_pre_enqueue batch=0 slot=0 count=0 peer=0000000000000000 outcome=ok start_unix_ns=$wait_end end_unix_ns=$((wait_end + 10)) duration_ns=10 request=0 attempt=0 tag=$tag" \
		"rhiza_latency stage=batcher_enqueue batch=0 slot=0 count=0 peer=0000000000000000 outcome=ok start_unix_ns=$((wait_end + 10)) end_unix_ns=$((wait_end + 20)) duration_ns=10 request=0 attempt=0 tag=$tag" \
		"rhiza_latency stage=batch_proposal batch=0 slot=$slot count=0 peer=0000000000000000 outcome=ok start_unix_ns=$proposal end_unix_ns=$((proposal + 100)) duration_ns=100 request=0 attempt=0 tag=$tag"
}

{
	emit_events 16 1000 1 1335 1050 12000 12100
	emit_events 32 2000 2 1335 8000 13000 13100
} > "$scratch/normal-trace.txt"
normal=$(awk -v client_file="$scratch/client.tsv" -f "$assess" "$scratch/normal-trace.txt")
grep -qx '# status=valid' <<<"$normal"
grep -qx '# valid=2' <<<"$normal"
grep -qx '# same_shard_wait_hold_overlaps=1' <<<"$normal"
grep -qx '# hold_end_semantics=timestamp_before_release_lower_bound' <<<"$normal"
grep -qx '# pre_enqueue_drops=0' <<<"$normal"
grep -qx '# lock_shard_out_of_range=0' <<<"$normal"

cp "$scratch/normal-trace.txt" "$scratch/drop-trace.txt"
printf '%s\n' 'rhiza_latency pre_enqueue_dropped=2' >> "$scratch/drop-trace.txt"
drop=$(awk -v client_file="$scratch/client.tsv" -f "$assess" "$scratch/drop-trace.txt")
grep -qx '# status=incomplete' <<<"$drop"
grep -qx '# pre_enqueue_drops=2' <<<"$drop"

awk '!($0 ~ /stage=pre_enqueue_encode / && $0 ~ /tag=16$/)' "$scratch/normal-trace.txt" > "$scratch/missing-trace.txt"
missing=$(awk -v client_file="$scratch/client.tsv" -f "$assess" "$scratch/missing-trace.txt")
grep -qx '# status=incomplete' <<<"$missing"
grep -qx '# missing_pre_enqueue_encode=1' <<<"$missing"
grep -q $'\tincomplete\tmissing_server_stage$' <<<"$missing"

cp "$scratch/normal-trace.txt" "$scratch/duplicate-trace.txt"
awk '$0 ~ /stage=pre_enqueue_clone / && $0 ~ /tag=16$/' "$scratch/normal-trace.txt" >> "$scratch/duplicate-trace.txt"
if awk -v client_file="$scratch/client.tsv" -f "$assess" "$scratch/duplicate-trace.txt" > "$scratch/duplicate.out"; then
	printf 'duplicate pre-enqueue event accepted\n' >&2
	exit 1
fi
grep -qx '# status=invalid' "$scratch/duplicate.out"
grep -qx '# duplicate_events=1' "$scratch/duplicate.out"

awk '{ if ($0 ~ /stage=request_lock_wait / && $0 ~ /tag=16$/) sub("count=1335", "count=4096"); print }' "$scratch/normal-trace.txt" > "$scratch/shard-range-trace.txt"
if awk -v client_file="$scratch/client.tsv" -f "$assess" "$scratch/shard-range-trace.txt" > "$scratch/shard-range.out"; then
	printf 'out-of-range lock shard accepted\n' >&2
	exit 1
fi
grep -qx '# status=invalid' "$scratch/shard-range.out"
grep -qx '# lock_shard_out_of_range=1' "$scratch/shard-range.out"

printf '16\t0\t1000\t9000\t1100\t8500\t7000\tfalse\n' > "$scratch/failed-client.tsv"
: > "$scratch/failed-trace.txt"
failed=$(awk -v client_file="$scratch/failed-client.tsv" -f "$assess" "$scratch/failed-trace.txt")
grep -qx '# status=errors' <<<"$failed"
grep -q $'\trequest_error\tclient_failure$' <<<"$failed"

printf '%s\n' "$normal" > "$scratch/valid.out"
printf '%s\n' "$drop" > "$scratch/drop.out"
printf '%s\n' "$missing" > "$scratch/missing.out"
printf '%s\n' "$failed" > "$scratch/failed.out"
bash "$here/../scripts/ci/check-preenqueue-assessment.sh" "$scratch/client.tsv" "$scratch/valid.out"
if bash "$here/../scripts/ci/check-preenqueue-assessment.sh" "$scratch/client.tsv" "$scratch/drop.out" >/dev/null 2>&1; then
	printf 'drop assessment passed causal validation\n' >&2
	exit 1
fi
if bash "$here/../scripts/ci/check-preenqueue-assessment.sh" "$scratch/client.tsv" "$scratch/missing.out" >/dev/null 2>&1; then
	printf 'missing assessment passed causal validation\n' >&2
	exit 1
fi
if bash "$here/../scripts/ci/check-preenqueue-assessment.sh" "$scratch/client.tsv" "$scratch/failed.out" >/dev/null 2>&1; then
	printf 'failed assessment passed causal validation\n' >&2
	exit 1
fi
: > "$scratch/empty.out"
if bash "$here/../scripts/ci/check-preenqueue-assessment.sh" "$scratch/client.tsv" "$scratch/empty.out" >/dev/null 2>&1; then
	printf 'missing assessment artifact passed causal validation\n' >&2
	exit 1
fi

for stage in pre_enqueue_clone pre_enqueue_validate pre_enqueue_fingerprint pre_enqueue_encode request_lock_wait request_lock_hold materializer_receipt_pre_enqueue batcher_enqueue; do
	grep -Fq "$stage" "$workflow"
done
grep -Fq 'pre_enqueue_dropped' "$workflow"
grep -Fq 'tag-collection.exit' "$workflow"
grep -Fq 'pre-enqueue-collection.exit' "$workflow"
grep -Fq 'check-preenqueue-assessment.sh' "$workflow"
! grep -Fq 'preflight-status.txt' "$workflow"
grep -Fq 'verify-sampled-diagnostic-artifacts.sh' "$workflow"
grep -Fq 'artifact-completeness.txt' "$workflow"

artifact_dir="$scratch/artifacts"
make_artifacts() {
	rm -rf "$artifact_dir"
	while IFS= read -r relative; do
		mkdir -p "$(dirname "$artifact_dir/$relative")"
		printf 'present\n' > "$artifact_dir/$relative"
	done < <("$verify_artifacts" --list)
	for phase in off-1 off-2; do
		for node in 0 1 2; do : > "$artifact_dir/$phase-node-$node-trace.txt"; done
	done
}

make_artifacts
"$verify_artifacts" "$artifact_dir"
grep -qx 'status=complete' "$artifact_dir/artifact-completeness.txt"
grep -qx 'empty=0' "$artifact_dir/artifact-completeness.txt"

: > "$artifact_dir/on-1-node-0-trace.txt"
if "$verify_artifacts" "$artifact_dir" >/dev/null 2>&1; then
	printf 'empty on-phase trace passed completeness validation\n' >&2
	exit 1
fi
grep -qx 'status=incomplete' "$artifact_dir/artifact-completeness.txt"
grep -qx 'empty=on-1-node-0-trace.txt' "$artifact_dir/artifact-completeness.txt"

make_artifacts
rm "$artifact_dir/off-2-node-1-trace.txt"
if "$verify_artifacts" "$artifact_dir" >/dev/null 2>&1; then
	printf 'missing off-phase trace passed completeness validation\n' >&2
	exit 1
fi
grep -qx 'missing=off-2-node-1-trace.txt' "$artifact_dir/artifact-completeness.txt"

make_artifacts
rm "$artifact_dir/on-1-pre-enqueue-latency.txt"
if "$verify_artifacts" "$artifact_dir" >/dev/null 2>&1; then
	printf 'missing required analysis artifact passed completeness validation\n' >&2
	exit 1
fi
grep -qx 'missing=on-1-pre-enqueue-latency.txt' "$artifact_dir/artifact-completeness.txt"

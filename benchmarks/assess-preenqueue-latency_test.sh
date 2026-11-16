#!/usr/bin/env bash
set -euo pipefail

here=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
assess="$here/../scripts/ci/assess-preenqueue-latency.awk"
pipeline="$here/../scripts/ci/assess-preenqueue-pipeline.sh"
versions="$here/../scripts/ci/capture-analysis-tool-versions.sh"
verify_artifacts="$here/../scripts/ci/verify-sampled-diagnostic-artifacts.sh"
workflow="$here/../.github/workflows/performance.yml"
gawk_fixture="$here/testdata/p99-preenqueue-gawk-5.2.1"
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
grep -qx '# pair_checks=2' <<<"$normal"
grep -qx '# pair_check_limit=4096' <<<"$normal"
grep -qx '# pair_limit_exceeded=0' <<<"$normal"

fixture_dir="$scratch/gawk-fixture"
mkdir -p "$fixture_dir"
cp "$gawk_fixture/client-tags.tsv" "$fixture_dir/on-1-client-tags.txt"
cp "$gawk_fixture/node-1-trace.txt" "$fixture_dir/on-1-node-1-trace.txt"
: > "$fixture_dir/on-1-node-0-trace.txt"
: > "$fixture_dir/on-1-node-2-trace.txt"
[[ $(wc -l < "$fixture_dir/on-1-client-tags.txt") -eq 48 ]]
[[ $(grep -c '^rhiza_latency stage=request_lock_wait ' "$fixture_dir/on-1-node-1-trace.txt") -eq 1 ]]
[[ $(grep -c '^rhiza_latency stage=request_lock_hold ' "$fixture_dir/on-1-node-1-trace.txt") -eq 47 ]]
fixture=$(awk -v client_file="$fixture_dir/on-1-client-tags.txt" -f "$assess" "$fixture_dir"/on-1-node-*-trace.txt)
grep -qx '# status=incomplete' <<<"$fixture"
grep -qx '# client_tags=48' <<<"$fixture"
grep -qx '# pair_checks=47' <<<"$fixture"
grep -qx '# pair_check_limit=4096' <<<"$fixture"
grep -qx '# pair_limit_exceeded=0' <<<"$fixture"
printf '%s\n' "$fixture" > "$scratch/fixture.out"
if bash "$here/../scripts/ci/check-preenqueue-assessment.sh" "$fixture_dir/on-1-client-tags.txt" "$scratch/fixture.out" >/dev/null 2>&1; then
	printf 'incomplete gawk fixture passed causal validation\n' >&2
	exit 1
fi

: > "$scratch/max-client.tsv"
: > "$scratch/max-trace.txt"
for tag in {1..64}; do
	base=$((tag * 10000))
	printf '%s\t0\t1000\t2000\t1001\t1999\t1000\ttrue\n' "$tag" >> "$scratch/max-client.tsv"
	emit_events "$tag" "$base" "$tag" 0 "$((base + 100))" "$((base + 110))" "$((base + 130))" >> "$scratch/max-trace.txt"
done
max_client=$(awk -v client_file="$scratch/max-client.tsv" -f "$assess" "$scratch/max-trace.txt")
grep -qx '# status=valid' <<<"$max_client"
grep -qx '# client_tags=64' <<<"$max_client"
grep -qx '# pair_checks=4032' <<<"$max_client"
grep -qx '# pair_limit_exceeded=0' <<<"$max_client"

printf '%s\n' \
	'rhiza_latency stage=request_lock_wait batch=0 slot=0 count=7 peer=0000000000000000 outcome=ok start_unix_ns=1000 end_unix_ns=5000 duration_ns=4000 request=0 attempt=0 tag=16' > "$scratch/boundary-a.txt"
printf '%s\n' \
	'rhiza_latency stage=request_lock_hold batch=0 slot=0 count=7 peer=0000000000000000 outcome=ok start_unix_ns=2000 end_unix_ns=6000 duration_ns=4000 request=0 attempt=0 tag=32' > "$scratch/boundary-b.txt"
different_files=$(awk -v client_file="$scratch/client.tsv" -f "$assess" "$scratch/boundary-a.txt" "$scratch/boundary-b.txt")
grep -qx '# same_shard_wait_hold_overlaps=0' <<<"$different_files"
cat "$scratch/boundary-a.txt" "$scratch/boundary-b.txt" > "$scratch/boundary-same-file.txt"
same_file=$(awk -v client_file="$scratch/client.tsv" -f "$assess" "$scratch/boundary-same-file.txt")
grep -qx '# same_shard_wait_hold_overlaps=1' <<<"$same_file"

awk 'BEGIN { for (i = 1; i <= 65; i++) print i "\t0\t1000\t2000\t1001\t1999\t1000\ttrue" }' > "$scratch/too-many-client-tags.tsv"
if awk -v client_file="$scratch/too-many-client-tags.tsv" -f "$assess" /dev/null > "$scratch/too-many.out"; then
	printf 'more than 64 client tags accepted\n' >&2
	exit 1
fi
grep -qx '# status=invalid too_many_client_tags' "$scratch/too-many.out"

cp "$scratch/normal-trace.txt" "$scratch/drop-trace.txt"
printf '%s\n' 'rhiza_latency pre_enqueue_dropped=212' >> "$scratch/drop-trace.txt"
drop=$(awk -v client_file="$scratch/client.tsv" -f "$assess" "$scratch/drop-trace.txt")
grep -qx '# status=incomplete' <<<"$drop"
grep -qx '# pre_enqueue_drops=212' <<<"$drop"
grep -qx '# pre_enqueue_dropped_unclassified=212' <<<"$drop"
grep -qx '# drop_reason_mismatch=0' <<<"$drop"
grep -qx '# pre_enqueue_dropped_reason_incomplete=0' <<<"$drop"

cp "$scratch/normal-trace.txt" "$scratch/typed-drop-trace.txt"
printf '%s\n' \
	'rhiza_latency pre_enqueue_dropped=212' \
	'rhiza_latency pre_enqueue_dropped_capacity=200' \
	'rhiza_latency pre_enqueue_dropped_trylock=12' \
	'rhiza_latency pre_enqueue_dropped_reason_incomplete=0' >> "$scratch/typed-drop-trace.txt"
typed_drop=$(awk -v client_file="$scratch/client.tsv" -f "$assess" "$scratch/typed-drop-trace.txt")
grep -qx '# status=incomplete' <<<"$typed_drop"
grep -qx '# pre_enqueue_dropped_capacity=200' <<<"$typed_drop"
grep -qx '# pre_enqueue_dropped_trylock=12' <<<"$typed_drop"
grep -qx '# pre_enqueue_dropped_unclassified=0' <<<"$typed_drop"
grep -qx '# drop_reason_mismatch=0' <<<"$typed_drop"
grep -qx '# pre_enqueue_dropped_reason_incomplete=0' <<<"$typed_drop"

cp "$scratch/normal-trace.txt" "$scratch/mismatched-drop-trace.txt"
printf '%s\n' \
	'rhiza_latency pre_enqueue_dropped=212' \
	'rhiza_latency pre_enqueue_dropped_capacity=200' \
	'rhiza_latency pre_enqueue_dropped_trylock=13' \
	'rhiza_latency pre_enqueue_dropped_reason_incomplete=0' >> "$scratch/mismatched-drop-trace.txt"
if awk -v client_file="$scratch/client.tsv" -f "$assess" "$scratch/mismatched-drop-trace.txt" > "$scratch/mismatched-drop.out"; then
	printf 'mismatched drop reason counts accepted\n' >&2
	exit 1
fi
grep -qx '# status=invalid' "$scratch/mismatched-drop.out"
grep -qx '# drop_reason_mismatch=1' "$scratch/mismatched-drop.out"

cp "$scratch/normal-trace.txt" "$scratch/incomplete-drop-reason-trace.txt"
printf '%s\n' \
	'rhiza_latency pre_enqueue_dropped=212' \
	'rhiza_latency pre_enqueue_dropped_capacity=200' \
	'rhiza_latency pre_enqueue_dropped_trylock=12' \
	'rhiza_latency pre_enqueue_dropped_reason_incomplete=1' >> "$scratch/incomplete-drop-reason-trace.txt"
if awk -v client_file="$scratch/client.tsv" -f "$assess" "$scratch/incomplete-drop-reason-trace.txt" > "$scratch/incomplete-drop-reason.out"; then
	printf 'incomplete drop reason counts accepted\n' >&2
	exit 1
fi
grep -qx '# status=invalid' "$scratch/incomplete-drop-reason.out"
grep -qx '# pre_enqueue_dropped_reason_incomplete=1' "$scratch/incomplete-drop-reason.out"

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
grep -Fq 'pre_enqueue_dropped_capacity' "$workflow"
grep -Fq 'pre_enqueue_dropped_trylock' "$workflow"
grep -Fq 'pre_enqueue_dropped_reason_incomplete' "$workflow"
for stage in proposal_mu_wait proposal_permit_wait proposal_worker_start; do
	grep -Fq "$stage" "$workflow"
done
for counter in proposal_gap_dropped proposal_gap_dropped_capacity proposal_gap_dropped_trylock proposal_gap_dropped_reason_incomplete; do
	grep -Fq "$counter" "$workflow"
done
grep -Fq 'tag-collection.exit' "$workflow"
grep -Fq 'pre-enqueue-collection.exit' "$workflow"
grep -Fq 'check-preenqueue-assessment.sh' "$workflow"
grep -Fq 'assess-preenqueue-pipeline.sh' "$workflow"
grep -Fq 'capture-analysis-tool-versions.sh' "$workflow"
grep -Fq 'analysis-tools.txt' "$workflow"
grep -Fq 'pre-enqueue-awk.exit' "$workflow"
grep -Fq 'pre-enqueue-sort.stderr' "$workflow"
if grep -Fq 'preflight-status.txt' "$workflow"; then
	printf 'diagnostic workflow unexpectedly retains preflight status\n' >&2
	exit 1
fi
grep -Fq 'verify-sampled-diagnostic-artifacts.sh' "$workflow"
grep -Fq 'artifact-completeness.txt' "$workflow"

pipeline_dir="$scratch/pipeline"
mkdir -p "$pipeline_dir"
cp "$scratch/client.tsv" "$pipeline_dir/on-1-client-tags.txt"
cp "$scratch/normal-trace.txt" "$pipeline_dir/on-1-node-1-trace.txt"
: > "$pipeline_dir/on-1-node-0-trace.txt"
: > "$pipeline_dir/on-1-node-2-trace.txt"
bash "$pipeline" "$pipeline_dir" on-1 awk sort
grep -qx '# status=valid' "$pipeline_dir/on-1-pre-enqueue-latency.txt"
grep -qx 0 "$pipeline_dir/on-1-pre-enqueue-awk.exit"
grep -qx 0 "$pipeline_dir/on-1-pre-enqueue-sort.exit"
[[ ! -s $pipeline_dir/on-1-pre-enqueue-awk.stderr ]]
[[ ! -s $pipeline_dir/on-1-pre-enqueue-sort.stderr ]]

leak_dir="$scratch/leak"
mkdir -p "$leak_dir"
printf '%s\t0\t1000\t2000\t1001\t1999\t1000\ttrue\n' 'SECRET_SENTINEL' > "$leak_dir/on-1-client-tags.txt"
: > "$leak_dir/on-1-node-0-trace.txt"
set +e
bash "$pipeline" "$leak_dir" on-1 awk sort
pipeline_rc=$?
set -e
[[ $pipeline_rc -eq 2 ]]
grep -qx 2 "$leak_dir/on-1-pre-enqueue-awk.exit"
grep -qx 0 "$leak_dir/on-1-pre-enqueue-sort.exit"
if grep -Eq 'SECRET_SENTINEL|rhiza_latency|request=|tag=' \
	"$leak_dir/on-1-pre-enqueue-awk.stderr" "$leak_dir/on-1-pre-enqueue-sort.stderr"; then
	printf 'sensitive input leaked into analyzer stderr\n' >&2
	exit 1
fi
if grep -Fq 'SECRET_SENTINEL' "$leak_dir/on-1-pre-enqueue-unsorted.txt" "$leak_dir/on-1-pre-enqueue-latency.txt"; then
	printf 'sensitive input leaked into analyzer output\n' >&2
	exit 1
fi

printf '%s\n' '#!/usr/bin/env bash' "printf 'partial\\n'" "printf 'mock-awk-stderr\\n' >&2" 'exit 134' > "$scratch/mock-awk"
printf '%s\n' '#!/usr/bin/env bash' 'cat' "printf 'mock-sort-stderr\\n' >&2" 'exit 7' > "$scratch/mock-sort"
chmod +x "$scratch/mock-awk" "$scratch/mock-sort"
set +e
bash "$pipeline" "$pipeline_dir" on-1 "$scratch/mock-awk" sort
pipeline_rc=$?
set -e
[[ $pipeline_rc -eq 134 ]]
grep -qx 134 "$pipeline_dir/on-1-pre-enqueue-awk.exit"
grep -qx 0 "$pipeline_dir/on-1-pre-enqueue-sort.exit"
grep -qx 'mock-awk-stderr' "$pipeline_dir/on-1-pre-enqueue-awk.stderr"
[[ ! -s $pipeline_dir/on-1-pre-enqueue-sort.stderr ]]
grep -qx 'partial' "$pipeline_dir/on-1-pre-enqueue-unsorted.txt"
grep -qx 'partial' "$pipeline_dir/on-1-pre-enqueue-latency.txt"

set +e
bash "$pipeline" "$pipeline_dir" on-1 awk "$scratch/mock-sort"
pipeline_rc=$?
set -e
[[ $pipeline_rc -eq 7 ]]
grep -qx 0 "$pipeline_dir/on-1-pre-enqueue-awk.exit"
grep -qx 7 "$pipeline_dir/on-1-pre-enqueue-sort.exit"
[[ ! -s $pipeline_dir/on-1-pre-enqueue-awk.stderr ]]
grep -qx 'mock-sort-stderr' "$pipeline_dir/on-1-pre-enqueue-sort.stderr"

printf '%s\n' '#!/usr/bin/env bash' "if [[ \${1:-} == -W ]]; then printf 'mock-awk-W-version\\n'; else printf 'mock-awk-long-version\\n'; fi" > "$scratch/mock-version-awk"
printf '%s\n' '#!/usr/bin/env bash' "printf 'mock-sort-version\\n'" > "$scratch/mock-version-sort"
chmod +x "$scratch/mock-version-awk" "$scratch/mock-version-sort"
bash "$versions" "$scratch/analysis-tools.txt" "$scratch/mock-version-awk" "$scratch/mock-version-sort"
grep -Fqx "awk_command=$scratch/mock-version-awk" "$scratch/analysis-tools.txt"
grep -Fqx 'awk_W_version_exit=0' "$scratch/analysis-tools.txt"
grep -Fqx 'mock-awk-W-version' "$scratch/analysis-tools.txt"
grep -Fqx 'mock-awk-long-version' "$scratch/analysis-tools.txt"
grep -Fqx 'sort_version_exit=0' "$scratch/analysis-tools.txt"
grep -Fqx 'mock-sort-version' "$scratch/analysis-tools.txt"

printf '%s\n' '#!/usr/bin/env bash' "if [[ \${1:-} == -W ]]; then printf 'mock-awk-W-failure\\n' >&2; exit 3; else printf 'mock-awk-long-failure\\n' >&2; exit 4; fi" > "$scratch/mock-failing-version-awk"
printf '%s\n' '#!/usr/bin/env bash' "printf 'mock-sort-failure\\n' >&2" 'exit 5' > "$scratch/mock-failing-version-sort"
chmod +x "$scratch/mock-failing-version-awk" "$scratch/mock-failing-version-sort"
bash "$versions" "$scratch/analysis-tools-failure.txt" "$scratch/mock-failing-version-awk" "$scratch/mock-failing-version-sort"
grep -Fqx 'awk_W_version_exit=3' "$scratch/analysis-tools-failure.txt"
grep -Fqx 'mock-awk-W-failure' "$scratch/analysis-tools-failure.txt"
grep -Fqx 'awk_version_exit=4' "$scratch/analysis-tools-failure.txt"
grep -Fqx 'mock-awk-long-failure' "$scratch/analysis-tools-failure.txt"
grep -Fqx 'sort_version_exit=5' "$scratch/analysis-tools-failure.txt"
grep -Fqx 'mock-sort-failure' "$scratch/analysis-tools-failure.txt"

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

: > "$artifact_dir/on-1-pre-enqueue-awk.stderr"
"$verify_artifacts" "$artifact_dir"
grep -qx 'status=complete' "$artifact_dir/artifact-completeness.txt"

make_artifacts
rm "$artifact_dir/on-1-pre-enqueue-sort.stderr"
if "$verify_artifacts" "$artifact_dir" >/dev/null 2>&1; then
	printf 'missing sort stderr passed completeness validation\n' >&2
	exit 1
fi
grep -qx 'missing=on-1-pre-enqueue-sort.stderr' "$artifact_dir/artifact-completeness.txt"

make_artifacts
: > "$artifact_dir/on-1-pre-enqueue-awk.exit"
if "$verify_artifacts" "$artifact_dir" >/dev/null 2>&1; then
	printf 'empty awk exit passed completeness validation\n' >&2
	exit 1
fi
grep -qx 'empty=on-1-pre-enqueue-awk.exit' "$artifact_dir/artifact-completeness.txt"

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

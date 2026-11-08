#!/usr/bin/env bash
set -euo pipefail
umask 077

failure_tail() {
	local log=$1
	[[ -s $log ]] || return 0
	printf 'BENCH_FAILURE_LOG %s (last 20 lines, sensitive lines omitted)\n' "${log##*/}"
	# These logs come only from the disposable fixture; never print auth material or URL queries.
	LC_ALL=C awk '
		/-----BEGIN .*PRIVATE KEY-----/ { pem=1 }
		{ lower=tolower($0) }
		pem || lower ~ /authorization|bearer|credential|password|private[_ -]?key|cookie|signature|secret|token["=: ]|[?][^[:space:]]+|https?:\/\/[^\/[:space:]]+@|eyj[a-z0-9_-]+\.[a-z0-9_-]+\./ {
			print "[sensitive line omitted]"
			if ($0 ~ /-----END .*PRIVATE KEY-----/) pem=0
			next
		}
		{ gsub(/[[:cntrl:]]/, " "); print substr($0, 1, 800) }
	' "$log" | tail -n 20
}

report_failure() {
	local status=$1 voter
	(( status != 0 )) || return 0
	printf 'BENCH_FAILURE status=%s evidence=%s\n' "$status" "${evidence:-unavailable}"
	if [[ -n ${evidence:-} ]]; then failure_tail "$evidence/apt.log"; fi
	if [[ -n ${sample:-} ]]; then
		failure_tail "$sample.log"
		for voter in 0 1 2; do failure_tail "$sample-node-$voter.log"; done
		failure_tail "$RHIZA_SERVER_BENCH_GCS_LOG"
	fi
}

trap 'status=$?; report_failure "$status" >&2 || true; exit "$status"' EXIT

sha=${RHIZA_EXPERIMENT_SHA:?RHIZA_EXPERIMENT_SHA must be the exact public commit}
[[ $sha =~ ^[0-9a-f]{40}$ ]]
comparison=${RHIZA_EXPERIMENT_LAYOUT-placement}
case $comparison in
	placement|storage-tier) ;;
	*) printf 'RHIZA_EXPERIMENT_LAYOUT must be placement or storage-tier\n' >&2; exit 2 ;;
esac
trial=$(mktemp -d /work/rhiza-voter-disk.XXXXXX)
evidence=$trial/evidence
export HOME=$trial/home GOCACHE=$trial/go-cache GOMODCACHE=$trial/go-mod
mkdir "$evidence" "$HOME" "$trial/bin"

if ! command -v jq >/dev/null; then
	# Download signed Debian packages without elevating privileges or installing globally.
	mkdir -p "$trial/apt/lists/partial" "$trial/apt/cache/archives/partial" "$trial/packages"
	apt_opts=(-o "Dir::State::lists=$trial/apt/lists" -o "Dir::Cache=$trial/apt/cache" -o APT::Sandbox::User= -o Debug::NoLocking=1)
	apt-get "${apt_opts[@]}" -qq update >"$evidence/apt.log" 2>&1
	(
		cd "$trial/packages"
		apt-get "${apt_opts[@]}" -qq download jq libjq1 libonig5
		for package in ./*.deb; do dpkg-deb -x "$package" "$trial/jq"; done
	) >>"$evidence/apt.log" 2>&1
	export PATH="$trial/jq/usr/bin:$PATH"
	export LD_LIBRARY_PATH="$trial/jq/usr/lib/x86_64-linux-gnu"
fi
jq --version

roots=(/mnt/disk-a /mnt/disk-b /mnt/disk-c)
declare -A devices=()
for root in "${roots[@]}"; do
	read -r target fs device < <(findmnt -nr -o TARGET,FSTYPE,MAJ:MIN -M "$root")
	if [[ $target != "$root" || ! -w $root || ! $fs =~ ^(ext4|xfs)$ || ! $device =~ ^[0-9]+:[0-9]+$ || ${devices[$device]+present} ]]; then
		printf 'Expected a distinct writable ext4/xfs mount: %s\n' "$root" >&2
		exit 1
	fi
	devices[$device]=1
	printf 'BENCH_MOUNT %s %s %s\n' "$root" "$fs" "$device"
done | tee "$evidence/mounts.txt"

git init -q "$trial/source"
git -C "$trial/source" fetch --quiet --depth=1 --no-tags https://github.com/mrchypark/rhiza.git "$sha"
git -C "$trial/source" checkout --quiet --detach FETCH_HEAD
cd "$trial/source"
[[ $(git rev-parse HEAD) == "$sha" ]]
CGO_ENABLED=0 go build -o "$trial/bin/rhiza" ./cmd/rhiza
CGO_ENABLED=0 go build -o "$trial/bin/rhiza-bench" ./cmd/rhiza-bench
server_sha=$(sha256sum "$trial/bin/rhiza" | awk '{print $1}')
client_sha=$(sha256sum "$trial/bin/rhiza-bench" | awk '{print $1}')
printf 'BENCH_ID sha=%s server=%s client=%s\n' "$sha" "$server_sha" "$client_sha" | tee "$evidence/identity.txt"

export RHIZA_SERVER_BENCH_GCS_LOG=/work/floci-access.log
export RHIZA_SERVER_BENCH_BINARY_DIR="$trial/bin" RHIZA_SERVER_BENCH_EXPECTED_SHA256="$server_sha:$client_sha"
export RHIZA_SERVER_BENCH_PROVIDER=gcs RHIZA_SERVER_BENCH_DURABILITY=async RHIZA_SERVER_BENCH_ASYNC_PUBLISH=1
export RHIZA_SERVER_BENCH_WORKLOAD=sql RHIZA_ASYNC_STAGE_PROBE=1
export RHIZA_SERVER_BENCH_REQUESTS=1000 RHIZA_SERVER_BENCH_CONCURRENCY=16
for ((pair = 0; pair < 6; pair++)); do
	a=${roots[$((pair % 3))]} b=${roots[$(((pair + 1) % 3))]} c=${roots[$(((pair + 2) % 3))]}
	arms=(shared split)
	if [[ $comparison == storage-tier ]]; then arms=(standard premium); fi
	if (( pair % 2 == 1 )); then arms=("${arms[1]}" "${arms[0]}"); fi
	for arm in "${arms[@]}"; do
		case $arm in
			shared) layout="$a:$a:$a" ;;
			split) layout="$a:$b:$c" ;;
			standard) disk=${roots[$(((pair / 2 + pair % 2) % 2))]}; layout="$disk:$disk:$disk" ;;
			premium) layout="${roots[2]}:${roots[2]}:${roots[2]}" ;;
		esac
		sample="$evidence/pair-$pair-$arm"
		printf 'BENCH_LAYOUT pair=%s arm=%s roots=%s\n' "$pair" "$arm" "$layout" | tee -a "$evidence/layouts.txt"
		status=0
		RHIZA_SERVER_BENCH_VOTER_ROOTS="$layout" bash benchmarks/run-server-reference.sh "$PWD" "$sample.json" >"$sample.log" 2>&1 || status=$?
		printf 'BENCH_EXIT pair=%s arm=%s status=%s evidence=%s\n' "$pair" "$arm" "$status" "$evidence"
		(( status == 0 )) || exit "$status"
		if ! jq -e --arg sha "$sha" --arg client "$client_sha" '
			.client_revision == $sha and .client_sha256 == $client and
			.workload == "sql" and .object_store_provider == "gcs" and .durability_mode == "async" and
			.ack_contract == "quorum WAL sync; object-store publication async" and
			.failure_mode == "healthy" and .concurrency == 16 and .runtime_failure_lines == 0 and
			.receipt_oracle == "sql-insert-id-v1" and
			.result.requests == 1000 and .result.successes == 1000 and .result.errors == 0 and
			.voter_resource_status == "measured" and (.voter_resources.nodes | length) == 3 and
			.write_object_store_delta.http_requests == 0 and
			.gcs_request_classes.write.observed_storage_responses == 0 and
			.gcs_request_classes.write.transport_minus_observed == 0 and
			.gcs_request_classes.publication.transport_minus_observed == 0 and
			.async_publication.forced == true and .async_publication.object_store_delta.http_requests > 0 and
			.async_publication.before.archive_tip < .async_publication.before.certified_tip and
			.async_publication.after.archive_tip >= .async_publication.before.certified_tip
		' "$sample.json" >/dev/null; then
			printf 'BENCH_GATE_FAILED pair=%s arm=%s evidence=%s\n' "$pair" "$arm" "$evidence" >&2
			exit 1
		fi
		printf 'BENCH_RESULT pair=%s arm=%s %s\n' "$pair" "$arm" "$(jq -c . "$sample.json")"
		awk -v prefix="BENCH_WINDOW pair=$pair arm=$arm " '/^ASYNC_WINDOW_(BEGIN|END)_NS=/ { print prefix $0; count++ } END { exit count != 2 }' "$sample.log"
		for voter in 0 1 2; do
			awk -v prefix="BENCH_STAGE pair=$pair arm=$arm voter=$voter " '
				/ASYNC_STAGE_PROBE_READY=1/ { ready=1; print prefix "ready=1" }
				/ASYNC_STAGE stage=/ { sub(/^.*ASYNC_STAGE stage=/, "stage="); print prefix $0 }
				END { exit !ready }
			' "$sample-node-$voter.log"
		done
	done
done
printf 'BENCH_COMPLETE pairs=6 samples=12 evidence=%s\n' "$evidence"

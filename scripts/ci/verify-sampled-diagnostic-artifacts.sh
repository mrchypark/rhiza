#!/usr/bin/env bash
set -euo pipefail

emit_required() {
	for relative in README.txt commit.txt calibration-status.txt analysis-tools.txt concurrency-status.txt concurrency-before.json concurrency-after.json; do
		printf 'nonempty\t%s\n' "$relative"
	done
	for phase in off-1 on-1 on-2 off-2; do
		for relative in "$phase.json" "$phase.exit" "$phase-build.txt" "$phase-sanitized.json"; do
			printf 'nonempty\t%s\n' "$relative"
		done
		kind=nonempty
		if [[ $phase == off-* ]]; then kind=exists; fi
		for node in 0 1 2; do printf '%s\t%s\n' "$kind" "$phase-node-$node-trace.txt"; done
	done
	for phase in on-1 on-2; do
		for relative in \
			"$phase-client-slots.txt" "$phase-slot-overlap.txt" "$phase-client-tags.txt" \
			"$phase.validation-exit" "$phase-tag-overlap.txt" "$phase-tag-collection.exit" \
			"$phase-tag-validation.exit" "$phase-pre-enqueue-latency.txt" \
			"$phase-pre-enqueue-awk.exit" "$phase-pre-enqueue-sort.exit" \
			"$phase-pre-enqueue-collection.exit" "$phase-pre-enqueue-validation.exit" \
			"$phase-live-progress.jsonl"; do
			printf 'nonempty\t%s\n' "$relative"
		done
		for relative in \
			"$phase-pre-enqueue-awk.stderr" "$phase-pre-enqueue-sort.stderr" \
			"$phase-pre-enqueue-unsorted.txt"; do
			printf 'exists\t%s\n' "$relative"
		done
	done
}

if [[ ${1:-} == --list ]]; then
	emit_required | cut -f2
	exit 0
fi
if [[ $# -ne 1 ]]; then
	printf 'usage: %s ARTIFACT_DIR|--list\n' "$0" >&2
	exit 2
fi

dir=$1
manifest="$dir/artifact-completeness.txt"
mkdir -p "$dir"
required=0
present=0
missing=0
empty=0
: > "$manifest"
while IFS=$'\t' read -r kind relative; do
	required=$((required + 1))
	if [[ ! -f $dir/$relative ]]; then
		printf 'missing=%s\n' "$relative" >> "$manifest"
		missing=$((missing + 1))
	elif [[ $kind == nonempty && ! -s $dir/$relative ]]; then
		printf 'empty=%s\n' "$relative" >> "$manifest"
		empty=$((empty + 1))
	else
		present=$((present + 1))
	fi
done < <(emit_required)

status=complete
if [[ $missing -ne 0 || $empty -ne 0 ]]; then status=incomplete; fi
{
	printf 'status=%s\n' "$status"
	printf 'required=%d\npresent=%d\nmissing=%d\nempty=%d\n' "$required" "$present" "$missing" "$empty"
} >> "$manifest"
if [[ $missing -ne 0 || $empty -ne 0 ]]; then exit 1; fi

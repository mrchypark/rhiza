#!/bin/sh
# Never allow subsequent resource-heavy tests if the per-run cleanup evidence
# is missing, contradictory, malformed, or cannot be read.
set -eu
: "${RHIZA_DIAGNOSTIC_DIR:?missing diagnostic directory}"
: "${RHIZA_DIAGNOSTIC_RAW_DIR:?missing raw directory}"
[ "${CURRENT_CLEANUP_CONFIRMED:-}" = true ] || exit 1
modes=${RHIZA_DIAGNOSTIC_MODES:-baseline expect}
case "$modes" in baseline|'baseline expect') ;; *) exit 1 ;; esac
for mode in $modes; do
    output=$RHIZA_DIAGNOSTIC_DIR/$mode
    raw=$RHIZA_DIAGNOSTIC_RAW_DIR/$mode
    for file in exit-code.txt verification-exit-code.txt cleanup-exit-code.txt; do
        target=$output/$file
        [ -f "$target" ] && [ ! -L "$target" ] || exit 1
        # One numeric line, with no duplicates or suffixes.
        awk 'NR != 1 || $0 !~ /^[0-9]+$/ {bad=1} END {if (NR != 1 || bad) exit 1}' "$target" || exit 1
    done
    [ "$(cat "$output/cleanup-exit-code.txt")" = 0 ] || exit 1
    count=0
    for run_dir in "$raw"/versity-*; do
        [ -d "$run_dir" ] && [ ! -L "$run_dir" ] || continue
        count=$((count + 1))
        marker=$run_dir/gateway-complete
        [ -f "$marker" ] && [ ! -L "$marker" ] || exit 1
        awk 'NR == 1 && $0 != "gateway_reaped=true" {bad=1}
             NR == 2 && $0 !~ /^child_status=(0|130)$/ {bad=1}
             NR == 3 && $0 != "forced=0" {bad=1}
             END {if (NR != 3 || bad) exit 1}' "$marker" || exit 1
    done
    [ "$count" -eq 1 ] || exit 1
done
printf '%s\n' 'Requested diagnostic gateways are confirmed reaped.'

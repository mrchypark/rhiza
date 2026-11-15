#!/bin/sh
# Diagnostic only. Preserve each test's actual status and run both cases even
# if the first fails; this does not change the SDK retry policy.
set -u

: "${RHIZA_VERSITYGW_BIN:?missing Versity binary}"
: "${RHIZA_DIAGNOSTIC_DIR:?missing diagnostic output directory}"
: "${RHIZA_DIAGNOSTIC_RAW_DIR:?missing isolated Versity evidence directory}"

aggregate=0
gate_ok=1
completed=0
mkdir -p "$RHIZA_DIAGNOSTIC_DIR" "$RHIZA_DIAGNOSTIC_RAW_DIR" || exit 1
for mode in baseline expect; do
    case "$mode" in
        baseline) selector=TestVersityConditionalLargePutEvidence; enabled=0; expect=false ;;
        expect) selector=TestVersityConditionalLargePutExpectEvidence; enabled=1; expect=true ;;
    esac
    output=$RHIZA_DIAGNOSTIC_DIR/$mode
    raw=$RHIZA_DIAGNOSTIC_RAW_DIR/$mode
    # Exclusive run directories prevent a marker from an earlier invocation
    # from authorizing a new run.
    if ! mkdir "$output" "$raw"; then
        printf 'diagnostic=%s setup_failed; command not started\n' "$mode" >&2
        aggregate=1
        gate_ok=0
        continue
    fi
    if ( unset GITHUB_OUTPUT; RHIZA_VERSITY_EVIDENCE_DIR="$raw" RHIZA_VERSITY_HTTP_TRACE=1 \
        RHIZA_VERSITY_EXPECT_CONTINUE="$enabled" \
        go test -count=1 -v -timeout=2m ./internal/objstore -run "^${selector}\$" ) >"$output/test.log" 2>&1; then
        status=0
    else
        status=$?
    fi
    if ! printf '%s\n' "$status" >"$output/exit-code.txt"; then
        printf 'diagnostic=%s cannot record actual exit=%s\n' "$mode" "$status" >&2
        aggregate=1
        gate_ok=0
    fi
    verification=0
    cleanup=0
    found=0
    for run_dir in "$raw"/versity-*; do
        [ -d "$run_dir" ] || continue
        if [ -L "$run_dir" ]; then cleanup=1; continue; fi
        found=$((found + 1))
        tries=0
        while [ ! -f "$run_dir/gateway-complete" ] && [ "$tries" -lt 100 ]; do
            sleep 0.1
            tries=$((tries + 1))
        done
        marker=$run_dir/gateway-complete
        [ -f "$marker" ] && [ ! -L "$marker" ] || cleanup=1
        awk 'NR == 1 && $0 != "gateway_reaped=true" {bad=1}
             NR == 2 && $0 !~ /^child_status=(0|130)$/ {bad=1}
             NR == 3 && $0 != "forced=0" {bad=1}
             END {if (NR != 3 || bad) exit 1}' "$marker" 2>/dev/null || cleanup=1
    done
    [ "$found" -eq 1 ] || cleanup=1
    if ! printf '%s\n' "$cleanup" >"$output/cleanup-exit-code.txt"; then
        printf 'diagnostic=%s cannot record cleanup exit=%s\n' "$mode" "$cleanup" >&2
        aggregate=1
        gate_ok=0
    fi
    grep -F -- "--- PASS: $selector " "$output/test.log" >/dev/null || verification=1
    grep -F "expect_continue_prototype=$expect http_trace_enabled=true" "$output/test.log" >/dev/null || verification=1
    grep -E 'VERSITY_HTTP_TRACE .*"attempts":[1-9][0-9]*' "$output/test.log" >/dev/null || verification=1
    grep -E "VERSITY_HTTP_TRACE .*\"event\":\"round_trip_started\".*\"expect_100_continue\":$expect" "$output/test.log" >/dev/null || verification=1
    if ! printf '%s\n' "$verification" >"$output/verification-exit-code.txt"; then
        printf 'diagnostic=%s cannot record verification exit=%s\n' "$mode" "$verification" >&2
        aggregate=1
        gate_ok=0
    fi
    printf 'diagnostic=%s exit=%s verification_exit=%s cleanup_exit=%s\n' "$mode" "$status" "$verification" "$cleanup"
    if ! cat "$output/test.log"; then
        printf 'diagnostic=%s could not report test log\n' "$mode" >&2
        aggregate=1
        gate_ok=0
    fi
    if [ "$status" -ne 0 ] || [ "$verification" -ne 0 ] || [ "$cleanup" -ne 0 ]; then
        aggregate=1
    fi
    if [ "$cleanup" -ne 0 ]; then
        gate_ok=0
        printf 'diagnostic=%s cleanup unconfirmed; remaining cases not started\n' "$mode" >&2
        break
    fi
    completed=$((completed + 1))
done
if [ "$gate_ok" -eq 1 ] && [ "$completed" -eq 2 ] && [ -n "${GITHUB_OUTPUT:-}" ]; then
    if ! printf '%s\n' 'cleanup_confirmed=true' >>"$GITHUB_OUTPUT"; then
        printf '%s\n' 'could not record current-run cleanup confirmation' >&2
        aggregate=1
    fi
fi
exit "$aggregate"

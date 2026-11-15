#!/bin/sh
set -eu
scope_pattern=$(sed -n "s/.*grep -Eq '\(.*\)'; then/\1/p" .github/workflows/performance.yml)
[ -n "$scope_pattern" ]
for changed_file in internal/objstore/providertransport_test.go internal/objstore/versity_conditional_put_test.go internal/objstore/conditional_put_ambiguity_test.go internal/objstore/versityfixture/fixture.go scripts/ci/versity-conditional-diagnostic.sh scripts/ci/versity-conditional-diagnostic-test.sh scripts/ci/versity-conditional-cleanup-gate.sh .github/workflows/performance.yml; do
    printf '%s\n' "$changed_file" | grep -Eq "$scope_pattern"
done
if printf '%s\n' docs/unrelated.md | grep -Eq "$scope_pattern"; then
    printf '%s\n' 'unrelated documentation unexpectedly selected the cost job' >&2
    exit 1
fi
test_dir=$(mktemp -d)
trap 'rm -rf "$test_dir"' EXIT HUP INT TERM
mkdir -p "$test_dir/bin"
cat >"$test_dir/bin/go" <<'MOCK'
#!/bin/sh
set -eu
mkdir -p "$RHIZA_VERSITY_EVIDENCE_DIR/versity-fixture"
if [ "${MOCK_BAD_CLEANUP:-}" = "$RHIZA_VERSITY_EXPECT_CONTINUE" ]; then
    printf '%s\n' 'gateway_reaped=false' 'child_status=0' 'forced=0' >"$RHIZA_VERSITY_EVIDENCE_DIR/versity-fixture/gateway-complete"
else
    printf '%s\n' 'gateway_reaped=true' 'child_status=0' 'forced=0' >"$RHIZA_VERSITY_EVIDENCE_DIR/versity-fixture/gateway-complete"
fi
case "$RHIZA_VERSITY_EXPECT_CONTINUE" in
    0) name=TestVersityConditionalLargePutEvidence; expected=false; mode=baseline ;;
    1) name=TestVersityConditionalLargePutExpectEvidence; expected=true; mode=expect ;;
    *) exit 99 ;;
esac
if [ "$mode" = baseline ] && [ -n "${MOCK_UNWRITABLE_STATUS:-}" ]; then
    mkdir -p "$RHIZA_DIAGNOSTIC_DIR/$mode/$MOCK_UNWRITABLE_STATUS"
fi
[ "$RHIZA_VERSITY_HTTP_TRACE" = 1 ] || exit 98
[ -z "${GITHUB_OUTPUT:-}" ] || exit 97
printf '%s\n' "$mode" >>"$MOCK_CALLS"
printf 'VERSITY_CONDITIONAL_PUT expect_continue_prototype=%s http_trace_enabled=true\n' "$expected"
if [ "${MOCK_MISSING_TRACE:-}" != "$mode" ]; then
    printf 'VERSITY_HTTP_TRACE {"attempts":1,"events":[{"event":"round_trip_started","expect_100_continue":%s}]}\n' "$expected"
fi
printf '%s\n' "--- PASS: $name (0.01s)"
if [ "${MOCK_FAIL:-}" = "$mode" ]; then exit 42; fi
MOCK
chmod +x "$test_dir/bin/go"
export PATH="$test_dir/bin:$PATH"
export RHIZA_VERSITYGW_BIN=/test/versity
export MOCK_CALLS="$test_dir/calls"
for scenario in success baseline expect missing status-write status-cleanup status-verification cleanup; do
    export RHIZA_DIAGNOSTIC_DIR="$test_dir/$scenario/output"
    export RHIZA_DIAGNOSTIC_RAW_DIR="$test_dir/$scenario/raw"
    export GITHUB_OUTPUT="$test_dir/$scenario-github-output"
    : >"$GITHUB_OUTPUT"
    : >"$MOCK_CALLS"
    MOCK_FAIL= MOCK_MISSING_TRACE= MOCK_BAD_CLEANUP= MOCK_UNWRITABLE_STATUS=
    case "$scenario" in
        baseline|expect) MOCK_FAIL=$scenario ;;
        missing) MOCK_MISSING_TRACE=expect ;;
        status-write) MOCK_UNWRITABLE_STATUS=exit-code.txt ;;
        status-cleanup) MOCK_UNWRITABLE_STATUS=cleanup-exit-code.txt ;;
        status-verification) MOCK_UNWRITABLE_STATUS=verification-exit-code.txt ;;
        cleanup) MOCK_BAD_CLEANUP=0 ;;
    esac
    export MOCK_FAIL MOCK_MISSING_TRACE MOCK_BAD_CLEANUP MOCK_UNWRITABLE_STATUS
    if sh scripts/ci/versity-conditional-diagnostic.sh >"$test_dir/$scenario.log" 2>&1; then status=0; else status=$?; fi
    CURRENT_CLEANUP_CONFIRMED=$(sed -n 's/^cleanup_confirmed=//p' "$GITHUB_OUTPUT")
    export CURRENT_CLEANUP_CONFIRMED
    if [ "$scenario" = cleanup ]; then expected_calls=1; else expected_calls=2; fi
    [ "$(wc -l <"$MOCK_CALLS" | tr -d ' ')" = "$expected_calls" ]
    [ "$(sed -n '1p' "$MOCK_CALLS")" = baseline ]
    if [ "$expected_calls" = 2 ]; then [ "$(sed -n '2p' "$MOCK_CALLS")" = expect ]; fi
    if [ "$scenario" = cleanup ]; then
        [ "$status" = 1 ]
        [ "$(cat "$RHIZA_DIAGNOSTIC_DIR/baseline/cleanup-exit-code.txt")" = 1 ]
        if sh scripts/ci/versity-conditional-cleanup-gate.sh >/dev/null; then exit 1; fi
        continue
    fi
    for mode in baseline expect; do
        if [ "$scenario:$mode" != status-write:baseline ]; then [ -f "$RHIZA_DIAGNOSTIC_DIR/$mode/exit-code.txt" ]; fi
        if [ "$scenario:$mode" != status-verification:baseline ]; then [ -f "$RHIZA_DIAGNOSTIC_DIR/$mode/verification-exit-code.txt" ]; fi
        if [ "$scenario:$mode" != status-cleanup:baseline ]; then [ "$(cat "$RHIZA_DIAGNOSTIC_DIR/$mode/cleanup-exit-code.txt")" = 0 ]; fi
    done
    case "$scenario" in
        success) [ "$status" = 0 ] ;;
        baseline|expect) [ "$status" = 1 ] && [ "$(cat "$RHIZA_DIAGNOSTIC_DIR/$scenario/exit-code.txt")" = 42 ] ;;
        missing) [ "$status" = 1 ] && [ "$(cat "$RHIZA_DIAGNOSTIC_DIR/expect/verification-exit-code.txt")" = 1 ] ;;
        status-write) [ "$status" = 1 ] && grep -F 'cannot record actual exit=0' "$test_dir/$scenario.log" >/dev/null ;;
        status-cleanup) [ "$status" = 1 ] && grep -F 'cannot record cleanup exit=0' "$test_dir/$scenario.log" >/dev/null ;;
        status-verification) [ "$status" = 1 ] && grep -F 'cannot record verification exit=0' "$test_dir/$scenario.log" >/dev/null ;;
    esac
    case "$scenario" in
        status-*) if sh scripts/ci/versity-conditional-cleanup-gate.sh >/dev/null; then exit 1; fi ;;
        *) sh scripts/ci/versity-conditional-cleanup-gate.sh >/dev/null ;;
    esac
done
# An earlier run's files cannot authorize this run when its output receipt is
# absent; neither malformed completion records nor missing statuses may pass.
export RHIZA_DIAGNOSTIC_DIR="$test_dir/success/output"
export RHIZA_DIAGNOSTIC_RAW_DIR="$test_dir/success/raw"
CURRENT_CLEANUP_CONFIRMED=
export CURRENT_CLEANUP_CONFIRMED
if sh scripts/ci/versity-conditional-cleanup-gate.sh >/dev/null; then exit 1; fi
CURRENT_CLEANUP_CONFIRMED=true
export CURRENT_CLEANUP_CONFIRMED
printf '%s\n' 'gateway_reaped=true' 'child_status=0' 'forced=0' 'gateway_reaped=false' >"$RHIZA_DIAGNOSTIC_RAW_DIR/baseline/versity-fixture/gateway-complete"
if sh scripts/ci/versity-conditional-cleanup-gate.sh >/dev/null; then exit 1; fi
printf '%s\n' 'gateway_reaped=true' 'child_status=0' 'forced=0' >"$RHIZA_DIAGNOSTIC_RAW_DIR/baseline/versity-fixture/gateway-complete"
rm "$RHIZA_DIAGNOSTIC_DIR/expect/cleanup-exit-code.txt"
if sh scripts/ci/versity-conditional-cleanup-gate.sh >/dev/null; then exit 1; fi
printf '%s\n' 'versity conditional diagnostic mock: PASS'

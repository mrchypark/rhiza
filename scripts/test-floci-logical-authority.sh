#!/bin/sh
set -eu

# Actions-only logical-authority GCS adapter harness. The dedicated workflow
# owns the pinned Floci-GCS service; this script never starts Docker and never
# permits a live cloud fallback.
if [ "${CI:-}" != "true" ] || [ "${RHIZA_LOGICAL_AUTHORITY_FLOCI:-}" != "1" ]; then
	echo "refusing logical-authority Floci test outside explicit CI opt-in" >&2
	exit 2
fi
if [ "${STORAGE_EMULATOR_HOST:-}" != "http://floci-gcs:4588" ] || [ "${GOOGLE_CLOUD_PROJECT:-}" != "floci-local" ]; then
	echo "refusing logical-authority test without the pinned Floci-GCS endpoint" >&2
	exit 2
fi
if [ -n "${GOOGLE_APPLICATION_CREDENTIALS:-}" ]; then
	echo "refusing logical-authority test with ambient Google credentials" >&2
	exit 2
fi

test_pattern='^(TestFlociGCSLogicalAuthorityStorageCAS|TestFlociGCSLogicalAuthorityThreeNodeBeforeAckAndSamePodRestart)$'
log_file=$(mktemp "${TMPDIR:-/tmp}/rhiza-logical-authority.XXXXXX")
keep_log=0
trap 'if [ "$keep_log" -eq 0 ]; then rm -f "$log_file"; fi' 0

# Go returns success when -run matches no tests, so verify the exact inventory
# before running and checking both required tests.
if go test -tags 'localhooks rhiza_local_testhooks' ./e2e/logical-authority -list "$test_pattern" >"$log_file" 2>&1; then
	list_status=0
else
	list_status=$?
fi
if [ "$list_status" -ne 0 ]; then
	cat "$log_file" >&2
	keep_log=1
	echo "test inventory command failed (exit $list_status); log preserved at $log_file" >&2
	exit "$list_status"
fi
for required_test in \
	TestFlociGCSLogicalAuthorityStorageCAS \
	TestFlociGCSLogicalAuthorityThreeNodeBeforeAckAndSamePodRestart
do
	if ! grep -Fxq "$required_test" "$log_file"; then
		cat "$log_file" >&2
		keep_log=1
		echo "required test $required_test is missing; inventory log preserved at $log_file" >&2
		exit 1
	fi
done

if go test -tags 'localhooks rhiza_local_testhooks' ./e2e/logical-authority -run "$test_pattern" -count=1 -timeout=8m -v >"$log_file" 2>&1; then
	test_status=0
else
	test_status=$?
fi
cat "$log_file"
if [ "$test_status" -ne 0 ]; then
	keep_log=1
	echo "Floci logical-authority tests failed (exit $test_status); log preserved at $log_file" >&2
	exit "$test_status"
fi

for required_test in \
	TestFlociGCSLogicalAuthorityStorageCAS \
	TestFlociGCSLogicalAuthorityThreeNodeBeforeAckAndSamePodRestart
do
	if ! grep -Fq -e "=== RUN   $required_test" "$log_file" || ! grep -Fq -e "--- PASS: $required_test" "$log_file"; then
		keep_log=1
		echo "required test $required_test did not run and pass; log preserved at $log_file" >&2
		exit 1
	fi
done
if grep -Fq -e '--- SKIP:' "$log_file"; then
	keep_log=1
	echo "Floci logical-authority tests unexpectedly skipped; log preserved at $log_file" >&2
	exit 1
fi

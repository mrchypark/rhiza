#!/bin/sh
# Focused FFI envelope and complete evidence inspection; no servers or ACK load.
set -eu
[ "$#" = 3 ] || { echo 'expected BASE_SHA CANDIDATE_SHA OUTPUT_DIR' >&2; exit 2; }
[ "${GITHUB_ACTIONS-}" = true ] || { echo 'GitHub Actions only' >&2; exit 2; }
: "${RUNNER_TEMP:?}"
repo=$(git rev-parse --show-toplevel)
base=$(git rev-parse --verify "$1^{commit}")
candidate=$(git rev-parse --verify "$2^{commit}")
output=$3
case "$output" in /*) ;; *) echo 'absolute output directory required' >&2; exit 2 ;; esac
[ ! -e "$output/json" ] || { echo 'JSON evidence already exists; refusing overwrite' >&2; exit 2; }
mkdir -p "$output/json"
work=$(mktemp -d "$RUNNER_TEMP/rhiza-json-bench.XXXXXX")
cleanup() {
  original=$?
  trap - EXIT HUP INT TERM
  printf '%s\n' "$original" > "$output/json/original.exit"
  cleanup_failed=0
  for label in base candidate; do
    if [ -e "$work/$label/.git" ]; then
      git -C "$repo" worktree remove --force "$work/$label" >> "$output/json/cleanup.txt" 2>&1 || cleanup_failed=1
    fi
    [ ! -e "$work/$label" ] || cleanup_failed=1
    for package in ffi evidence; do
      if [ -f "$work/$label-$package.test" ]; then rm -- "$work/$label-$package.test" || cleanup_failed=1; fi
    done
  done
  rmdir "$work" >> "$output/json/cleanup.txt" 2>&1 || cleanup_failed=1
  [ ! -e "$work" ] || cleanup_failed=1
  printf '%s\n' "$cleanup_failed" > "$output/json/cleanup.exit"
  final=$original
  if [ "$final" = 0 ] && [ "$cleanup_failed" != 0 ]; then final=1; fi
  printf '%s\n' "$final" > "$output/json/final.exit"
  exit "$final"
}
trap cleanup EXIT
trap 'exit 1' HUP INT TERM
{
  printf 'scope=go-ffi-envelope-and-complete-evidence-inspection\nbase_sha=%s\ncandidate_sha=%s\nworkflow_sha=%s\n' "$base" "$candidate" "${GITHUB_SHA-}"
  printf 'base_tree=%s\ncandidate_tree=%s\n' "$(git rev-parse "$base^{tree}")" "$(git rev-parse "$candidate^{tree}")"
  printf 'samples=10\nffi_benchtime=250ms\nevidence_benchtime=1s\ngomaxprocs=2\ncgo=1\ngoexperiment=cgocheck2\n'
  printf 'evidence_scope=includes-SQLite-sort-hash-IO-cleanup-not-JSON-alone\n'
  printf 'go=%s\nrunner_image=%s-%s\n' "$(go version)" "${ImageOS-unknown}" "${ImageVersion-unknown}"
  printf 'go_cc=%s\n' "$(go env CC)"
  cc --version
  uname -srvmo
  lscpu
} > "$output/environment.txt"
ffi_fixture=cmd/rhiza-ffi/json_envelope_test.go
evidence_fixture=pkg/materializer/application_evidence_benchmark_test.go
check_source() {
  if [ "$1" = base ]; then expected=$base; else expected=$candidate; fi
  test "$(git -C "$work/$1" rev-parse HEAD)" = "$expected"
  git -C "$work/$1" diff --exit-code HEAD -- . ":(exclude)$ffi_fixture" ":(exclude)$evidence_fixture" > "$output/json/$1-source-$2.diff"
  untracked_files=$(git -C "$work/$1" ls-files --others --exclude-standard)
  printf '%s\n' "$untracked_files" | while IFS= read -r untracked; do
    [ -n "$untracked" ] || continue
    case "$untracked" in "$ffi_fixture"|"$evidence_fixture") ;; *) echo 'unexpected untracked source file' >&2; exit 1 ;; esac
  done
}
git cat-file -e "$candidate:$ffi_fixture"
git cat-file -e "$candidate:$evidence_fixture"
for label in base candidate; do
  if [ "$label" = base ]; then revision=$base; else revision=$candidate; fi
  git -C "$repo" worktree add --detach "$work/$label" "$revision" >> "$output/json/worktrees.txt" 2>&1
  # Only this identical test fixture is overlaid, never production source.
  for fixture in "$ffi_fixture" "$evidence_fixture"; do
    git show "$candidate:$fixture" > "$work/$label/$fixture"
    sha256sum "$work/$label/$fixture" >> "$output/json/fixture-hashes.txt"
  done
  check_source "$label" before
  for package in ffi evidence; do
    if [ "$package" = ffi ]; then
      go_package=./cmd/rhiza-ffi
      regression='^Test(DataJSONMatchesLegacyMapBytes|JSONEnvelopeInputSizeLimitUnchanged)$'
      names='TestDataJSONMatchesLegacyMapBytes TestJSONEnvelopeInputSizeLimitUnchanged'
    else
      go_package=./pkg/materializer
      regression='^TestApplicationEvidenceCostFixture$'
      names=TestApplicationEvidenceCostFixture
    fi
    binary="$work/$label-$package.test"
    status=0
    (cd "$work/$label" && CGO_ENABLED=1 GOEXPERIMENT=cgocheck2 go test -c -o "$binary" "$go_package") > "$output/json/$label-$package-build.txt" 2>&1 || status=$?
    printf '%s\n' "$status" > "$output/json/$label-$package-build.exit"
    [ "$status" = 0 ] || exit "$status"
    sha256sum "$binary" >> "$output/json/binary-hashes.txt"
    "$binary" -test.list "$regression" > "$output/json/$label-$package-tests.txt"
    for test_name in $names; do grep -Fx "$test_name" "$output/json/$label-$package-tests.txt" >/dev/null; done
    status=0
    "$binary" -test.run "$regression" -test.count 1 -test.timeout 1m > "$output/json/$label-$package-regression.txt" 2>&1 || status=$?
    printf '%s\n' "$status" > "$output/json/$label-$package-regression.exit"
    [ "$status" = 0 ] || exit "$status"
  done
done
for fixture in "$ffi_fixture" "$evidence_fixture"; do
  test "$(sha256sum "$work/base/$fixture" | cut -d ' ' -f1)" = "$(sha256sum "$work/candidate/$fixture" | cut -d ' ' -f1)"
done
: > "$output/base.txt"
: > "$output/candidate.txt"
sample=1
while [ "$sample" -le 10 ]; do
  if [ $((sample % 2)) = 1 ]; then order='base candidate'; else order='candidate base'; fi
  for label in $order; do
    for package in ffi evidence; do
      raw="$output/json/$label-$package-sample-$sample.txt"
      if [ "$package" = ffi ]; then selector='^BenchmarkFFIDataJSONEnvelope$'; benchtime=250ms
      else selector='^BenchmarkApplicationEvidenceScan$'; benchtime=1s; fi
      status=0
      (cd "$work/$label" && GOMAXPROCS=2 "$work/$label-$package.test" -test.run '^$' -test.bench "$selector" -test.benchtime "$benchtime" -test.count 1 -test.benchmem -test.cpu 2 -test.timeout 2m) > "$raw" 2>&1 || status=$?
      printf '%s\n' "$status" > "$output/json/$label-$package-sample-$sample.exit"
      [ "$status" = 0 ] || exit "$status"
      [ "$(grep -Ec '^Benchmark[^[:space:]]+[[:space:]]+[0-9]+' "$raw")" = 8 ]
      if [ "$package" = ffi ]; then
        for case_name in null receipt blob_1k sql_rows; do
          for variant in legacy_map production; do
            [ "$(grep -Ec "^BenchmarkFFIDataJSONEnvelope/$case_name/$variant(-[1-9][0-9]*)?[[:space:]]" "$raw")" = 1 ]
          done
        done
      else
        for profile in sql sql_kv; do
          for rows in 0 64 512 4096; do
            [ "$(grep -Ec "^BenchmarkApplicationEvidenceScan/$profile/rows_$rows/blob_256(-[1-9][0-9]*)?[[:space:]]" "$raw")" = 1 ]
          done
        done
      fi
      cat "$raw" >> "$output/$label.txt"
    done
  done
  sample=$((sample+1))
done
for label in base candidate; do
  check_source "$label" after
  for fixture in "$ffi_fixture" "$evidence_fixture"; do
    test "$(git hash-object "$work/$label/$fixture")" = "$(git rev-parse "$candidate:$fixture")"
    sha256sum "$work/$label/$fixture" >> "$output/json/fixture-hashes-after.txt"
  done
done
sha256sum "$output/base.txt" "$output/candidate.txt" > "$output/json/result-hashes.txt"
wc -c "$output/base.txt" "$output/candidate.txt" > "$output/json/result-bytes.txt"

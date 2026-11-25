#!/bin/sh
set -eu

# The runner passes immutable commits and a private evidence directory.
base=$1
candidate=$2
evidence=$3
case "$evidence" in /*) ;; *) echo 'evidence path must be absolute' >&2; exit 1;; esac
mkdir -p "$evidence"
fixture=$(pwd -P)/pkg/node/local_compaction_ci_evidence_test.go
tmp=$(mktemp -d "${RUNNER_TEMP:?}/rhiza-413.XXXXXX")
cleanup() {
  git worktree remove --force "$tmp/base" >/dev/null 2>&1 || true
  git worktree remove --force "$tmp/candidate" >/dev/null 2>&1 || true
  rmdir "$tmp" >/dev/null 2>&1 || echo "temporary worktree directory remains: $tmp" >&2
}
trap cleanup EXIT HUP INT TERM
git worktree add --detach "$tmp/base" "$base"
git worktree add --detach "$tmp/candidate" "$candidate"
for name in base candidate; do
  dir=$tmp/$name
  git -C "$dir" rev-parse HEAD > "$evidence/$name.sha"
  cp "$fixture" "$dir/pkg/node/local_compaction_ci_evidence_test.go"
  # Add only a nonblocking timestamp hook at the same build boundary in both
  # revisions. The existing end hook is unchanged; neither hook pauses I/O.
  awk '
    /^func \(c \*Compaction\) Build\(\)/ { in_build=1 }
    in_build && /w := c\.wal/ { print; print "\thitLocalWALCrashBoundary(\"before-compaction-build\")"; in_build=0; count++; next }
    { print }
    END { if (count != 1) exit 1 }
  ' "$dir/pkg/qlog/wal.go" > "$tmp/$name-wal.go"
  mv "$tmp/$name-wal.go" "$dir/pkg/qlog/wal.go"
  shasum -a 256 "$dir/pkg/qlog/wal.go" "$dir/pkg/node/local_compaction_ci_evidence_test.go" > "$evidence/$name-overlay.sha256"
done
cmp "$tmp/base/pkg/node/local_compaction_ci_evidence_test.go" "$tmp/candidate/pkg/node/local_compaction_ci_evidence_test.go"
overall=0
p99_comparable=true
summaries_valid=true
for item in base-1 candidate-1 candidate-2 base-2; do
  name=${item%-*}
  echo "running $item"
  status=0
  (cd "$tmp/$name" && RHIZA_LOCAL_COMPACTION_EVIDENCE=1 CGO_ENABLED=1 go test -v -count=1 -tags rhiza_local_testhooks -timeout=10m ./pkg/node -run '^TestLocalCompactionCIEvidence$') > "$evidence/$item.log" 2>&1 || status=$?
  printf '%s\n' "$status" > "$evidence/$item.exit"
  if [ "$status" -ne 0 ]; then cat "$evidence/$item.log"; overall=1; fi
  rg 'LOCAL_COMPACTION_(EVIDENCE|RESTORE)' "$evidence/$item.log" || true
  sed -n 's/^.*LOCAL_COMPACTION_EVIDENCE //p' "$evidence/$item.log" > "$evidence/$item.summary.json"
  if ! jq -e -s -f scripts/ci-local-compaction-summary.jq "$evidence/$item.summary.json" > /dev/null; then
    echo "$item: missing or invalid measurement summary" >&2
    overall=1
    p99_comparable=false
    summaries_valid=false
  elif ! jq -e '.p99_comparable == true' "$evidence/$item.summary.json" > /dev/null; then
    p99_comparable=false
  fi
done
if [ "$overall" -ne 0 ]; then p99_comparable=false; fi
printf '{"summaries_valid":%s,"p99_comparable":%s,"reason":"requires valid summaries, at least 100 successful overlapping samples per run, and successful restore"}\n' "$summaries_valid" "$p99_comparable" | tee "$evidence/comparison-validity.json"
exit "$overall"

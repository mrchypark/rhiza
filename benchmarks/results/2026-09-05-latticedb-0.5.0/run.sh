#!/usr/bin/env bash
set -euo pipefail
cd "$(git rev-parse --show-toplevel)"
out="$(pwd)/benchmarks/results/2026-09-05-latticedb-0.5.0"
work=${RHIZA_EVAL_WORK:-$(mktemp -d /tmp/rhiza-lattice-eval.XXXXXX)}
mkdir -p "$work"
if [[ ${RHIZA_EVAL_PREBUILT:-0} != 1 ]]; then
  cp go.mod "$work/baseline.mod"
  cp go.sum "$work/baseline.sum"
  go mod edit -modfile="$work/baseline.mod" -require=github.com/mrchypark/latticedb-go@v0.3.0
  go mod download -modfile="$work/baseline.mod" github.com/mrchypark/latticedb-go
  for package in materializer network; do
    CGO_ENABLED=0 go test -c -modfile="$work/baseline.mod" -o "$work/$package-030.test" "./pkg/$package"
    CGO_ENABLED=0 go test -c -o "$work/$package-050.test" "./pkg/$package"
  done
fi
: > "$out/030.txt"
: > "$out/050.txt"
for sample in {1..10}; do
  versions=(030 050)
  if (( sample % 2 == 0 )); then versions=(050 030); fi
  for version in "${versions[@]}"; do
    /usr/bin/time -l -o "$out/$version-resources-$sample.txt" env GOMAXPROCS=2 "$work/materializer-$version.test" -test.run '^$' \
      -test.bench '^Benchmark(GraphApply|LatticeAppMetadataUpdate4096Keys|GraphQuery4096Nodes)$' \
      -test.benchtime 100x -test.count 1 -test.benchmem -test.cpu 2 >> "$out/$version.txt"
    GOMAXPROCS=2 "$work/materializer-$version.test" -test.run '^$' \
      -test.bench '^BenchmarkGraphSnapshotFreezeByDatabaseSize$' \
      -test.benchtime 10x -test.count 1 -test.benchmem -test.cpu 2 >> "$out/$version.txt"
    GOMAXPROCS=2 "$work/network-$version.test" -test.run '^$' \
      -test.bench '^BenchmarkHTTPQueryLoopback$' \
      -test.benchtime 1000x -test.count 1 -test.benchmem -test.cpu 2 >> "$out/$version.txt"
  done
  printf 'sample %d/10 complete\n' "$sample"
done
GOBIN="$work" go install golang.org/x/perf/cmd/benchstat@v0.0.0-20260825160852-19be9d8e6c70
"$work/benchstat" "$out/030.txt" "$out/050.txt" > "$out/benchstat.txt"

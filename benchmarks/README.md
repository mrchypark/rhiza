# Benchmarks

Store durable benchmark evidence under `benchmarks/results/<date>-<comparison>/`.
Each result directory contains:

- `environment.json`: commits, dirty-tree fingerprints, image digests, runtime, and matrix.
- `raw/`: immutable NDJSON and Go benchmark output.
- `resources/`: pod CPU/cgroup and memory snapshots.
- `summary.json` and `summary.csv`: medians derived from raw HTTP samples.
- `comparison.json`: baseline versus candidate deltas.
- `REPORT.md`: conclusions and known limitations.

Do not store container images, databases, profiles containing user data, or object-store payloads here.
Commit the text artifacts so performance claims remain reviewable with the code that produced them.

Run one Dory profile with:

```bash
bash benchmarks/run-dory-profile.sh \
  sql rhiza-e2e:dev async current-sql-async benchmarks/results/<run>
```

Set `RHIZA_BENCH_CHECKPOINT_INTERVAL=1s` to measure checkpoint interference. Add
`one-fault` as the sixth argument to measure with one peer unavailable. The runner
uses a fresh object prefix and empty pod volumes for every invocation. Override
the default one-minute async publication timer with
`RHIZA_BENCH_SYNC_INTERVAL=10m` when a quiet profile may run longer than a minute.

Aggregate all NDJSON files with:

```bash
jq -s -f benchmarks/summarize.jq benchmarks/results/<run>/raw/*.ndjson \
  > benchmarks/results/<run>/summary.json
```

## CI performance comparison

`.github/workflows/performance.yml` compares every performance-relevant pull
request with its base commit on the same fixed `ubuntu-24.04` runner. It builds
the benchmark binaries before measurement, pins `GOMAXPROCS=2`, and alternates
base and candidate execution order across ten samples. The Job Summary contains
three-peer `ExecuteReturning` measurements for 1 row, 100 rows, and a
near-1-MiB result through the in-process server API, plus the `benchstat`
comparison. Hiqlite's verified result is reused from
`hiqlite-reference.json` while its pinned commit and benchmark patch are
unchanged. CI checks Hiqlite's remote release tags on every run and only reuses
the result when its recorded version is still latest; a newer release or a
changed patch requires a fresh reference run and an updated JSON record.
Current-run Rhiza output, the reused reference, and runner provenance are
uploaded as a 30-day artifact. Until this workflow first
lands on `main`, its bootstrap run uses the candidate's previous commit so both
sides contain the same benchmark harness; later pull requests compare against
their actual base commit.

The Rhiza server qualification injects `SIGKILL` into each named voter while
the 100,000-write workload is running. QuePaxa has no stable leader role, so
results are reported by peer identity (`n0`, `n1`, `n2`) with throughput, p99,
maximum request latency, retries, and a linearizable final row-count check.
Zero final request errors across all three runs is the availability gate. Node
logs are always uploaded, and each result counts lines containing `error`,
`failed`, or `timeout` so internal failure noise is visible beside client
correctness. The known quic-go host UDP receive-buffer warning is excluded from
that application-failure count. A run fails when the count is nonzero, or when
a fault-injection request exceeds the configurable 1,500 ms maximum latency
gate (`RHIZA_SERVER_BENCH_MAX_FAULT_LATENCY_MS`).

Hiqlite remains an external Raft reference, not a direct algorithm comparison.
Its leader/follower cases gracefully stop one peer and wait for a replacement
leader before measurement, so they describe post-failover steady state rather
than failover interruption. The pinned source patch is kept in
`benchmarks/hiqlite-one-peer.patch`; CI verifies its recorded digest before
reusing the result.

The workflow is advisory: candidate benchmark failures fail the job, while a
measured regression is reported without an arbitrary threshold. A failing
baseline benchmark is retained as evidence and does not prevent the fixed
candidate from running. Use `workflow_dispatch` to compare the selected commit
against another base ref. The same collector can be smoke-tested locally with
short samples:

```bash
RHIZA_BENCH_COUNT=1 RHIZA_BENCH_TIME=100ms \
  benchmarks/run-ci-benchmarks.sh HEAD HEAD /tmp/rhiza-performance
```

Hosted-runner absolute throughput remains diagnostic. Use paired deltas for PR
decisions and retain the Dory matrix for Kubernetes, fault, and object-store
qualification.

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

Issue-184 measurements run in that same paired job with identical benchmark
fixtures added to both revisions. They report latency and Go allocation metrics
(`-benchmem`) for retained durability prefixes (1/64/1,024 slots), SQL
authorizer catalog sizes (1/64/256 tables), and graph pruning (256-slot
windows, populated every 1/8 slots, with 1/8 requests per populated slot).
Graph pruning uses one measured operation per sample because its untimed setup
repopulates the graph metadata; the job still collects ten interleaved samples
per revision. Existing `BenchmarkSQLBatchApply` continues to cover SQL command
batch sizes 1/8/32/64/128. These are measurements only: do not infer a production
bottleneck or optimization from the matrix without paired results.

Hosted-runner absolute throughput remains diagnostic. Use paired deltas for PR
decisions and retain the Dory matrix for Kubernetes, fault, and object-store
qualification.

## Issue #185 local S3 cost evidence

Use the `Performance` workflow's `workflow_dispatch` input
`issue185_cost_evidence=true` to run the opt-in #185 evidence job. It is
separate from the ordinary paired benchmark job and runs the provider-backed
fixtures serially on a `macos-15` arm64 runner. The job downloads the official
Versity Gateway v1.8.0 Darwin arm64 archive and verifies its SHA-256
(`4953096f65a9c0d62ab184fb6b2ba7c2435229205cf00a56cb62cd4bf6b216ca`) before
starting tests. Each S3 fixture starts its own loopback gateway with a fresh
temporary POSIX root and generated test credentials; it does not use ambient
credentials or cloud storage.

The run selects `TestVersityGatewayCostEvidence` and
`TestVersitySDKRetryEvidence`, followed by `TestCheckpointCost`,
`TestArchiveGCCost`, and
`TestCheckpointRestoreCostExercisesNodeRecoveryPathS3`. It then runs the
build-tagged `TestNodeOpenCatchUpRepairsHeldLearnGapForConcurrentKVCalls`
lifecycle regression and one `TestNodeForegroundAPICostS3` baseline, followed by
the bare-core `pkg/network` `TestForegroundAPICost` component diagnostic
serially for `baseline`, `checkpoint-active`, and
`gc-active`, three repetitions each. Each foreground repetition retains its
30-second warm-up and 120-second measured window. The restore case publishes a
certified checkpoint through one node, removes only the local SQLite files,
then verifies that a second `Node.Open` recovers the value and reaches ready.

The held-gap regression uses async object-store durability to isolate automatic
Node catch-up and ACK-after-apply behavior; it is mechanism evidence, not a
foreground cost result. `TestNodeForegroundAPICostS3` uses the production
Before-ACK API path through three opened Nodes and preserves the same 4096-key,
16-worker, 30-second warm-up, 120-second measurement, and 30-second call bounds.
It reports per-phase logical API counters, per-Node object-store counter deltas,
and independent Versity access-log request counts. These Node-path results are
separate from the existing checkpoint publication and archive-GC selectors;
they do not replace their metrics or establish checkpoint/GC maintenance
performance. Both new Node selectors are opt-in within the explicitly
dispatched evidence job and require the existing `rhiza_local_testhooks` tag.
The tagged Node run also retains only per-phase transport-error aggregates and
the first sanitized failure detail (request method/status family, error class,
and elapsed time), including checkpoint cleanup preparation and each sequential
Node shutdown; phase is sampled at request start. It does not log URLs, keys,
headers, credentials, or bodies. This is diagnostic attribution, not a claim
that retry metadata is available.
The nine `pkg/network` repetitions exercise the network component directly;
they are not Node API foreground measurements and are labeled
`network-component-*` in the evidence artifact. The three-Node Before-ACK
baseline is the actual Node API foreground result.

For the foreground result, each fixed window is an enrollment cutoff: no new
API calls start after it. Calls already admitted drain under a parent-bounded
30-second per-call context, with workers joined before the next phase. The
seed budget reserves a possible 30-second drain for both the 30-second warm-up
and 120-second measured window, plus the existing teardown reserve.
Within `put` and `linearizable_get`, `started_requests` counts all admitted
calls, including calls that complete during drain; `requests`, success, error,
timeout, throughput, completion-p99, and
`sample_recording_ns_per_operation` fields describe completions inside the
measured window. Their `drain_*` fields retain late completions, errors, timeouts,
`ErrCommitUnknown` outcomes, and drain latency separately; these outcomes are
not discarded or treated as failed commits, and do not inflate in-window
throughput. `window_boundary` reconciles starts, in-window completions, drain
completions, outstanding calls at cutoff, and outstanding calls after join.

The artifact `issue-185-cost-evidence-<run_id>` includes the exact commands,
per-command exit codes, environment and binary provenance, Go output, and raw
Versity access records. Server request records and client transport-attempt
counters are reported independently. Retry attribution remains unknown when
the SDK supplies no recognized attempt metadata; it must not be reported as
zero. The retry control fixture seeds an object during setup, then injects one
503 through a local reverse proxy on its first GET. With MinIO `MaxRetries=2`
(two total request attempts for the pinned client), it expects two client GET
attempts, one injected failure, and one GET reaching the Versity backend. The
proxy counter proves this controlled GET retry; missing MinIO attempt metadata
remains unknown and is not presented as zero retries. It does not measure PUT
retries or upload-body replay. Upload-attempt bytes, per-upload acknowledged
bytes, and winner/root reachable bytes are distinct measures. `BytesUploaded`
is SDK source-reader work: it includes reader re-reads used for hashing or
retries and is not physical wire bytes. `BytesPublished` adds the known object
size once after a successful upload; when size is unknown it falls back to
source-reader bytes consumed, so that fallback is not an exact unique-object
byte measure. `HTTPRequestBodyBytes` is a separate transport-body count and can
include S3 framing/signatures. The Node result's legacy
`attempted_upload_bytes` label refers to `BytesUploaded` with this reader-work
meaning. The local Versity results are local S3 compatibility evidence only—not
AWS/GCS qualification or a release gate.

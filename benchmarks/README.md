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
The final query requires both N total rows and N rows in `[0,N)`; with the
INTEGER PRIMARY KEY this proves the exact requested identity set. The client
also validates the committed SQL INSERT receipt, affected-row count, inserted
ID, and receipt retention bound. It does not assume SQL exposes an `Applied`
flag or echoes the request ID, and this workload-specific oracle is not a
general SQL response validator.
Zero final request errors across all three runs is the availability gate. Node
logs are always uploaded, and each result counts lines containing `error`,
`failed`, or `timeout` so internal failure noise is visible beside client
correctness. The known quic-go host UDP receive-buffer warning is excluded from
that application-failure count. A run fails when the count is nonzero, or when
a fault-injection request exceeds the configurable 1,500 ms maximum latency
gate (`RHIZA_SERVER_BENCH_MAX_FAULT_LATENCY_MS`).

Fault lanes are Linux-only and require a kernel pidfd. Before requests start,
the parent verifies its running child and captures its `/proc` start-time ticks;
the client checks the same birth/parent identity and pins it with `PidfdOpen`.
The nonce-protected loopback `/fault` endpoint accepts no body or target PID,
makes one `PidfdSendSignal(SIGKILL)` attempt, and records the syscall bracket in
the same monotonic clock as request sent/finished timestamps. No numeric-PID
signal fallback is permitted. Unsupported kernels/platforms fail closed.
The parent must also observe Bash child wait status **137**; other statuses,
including normal exit 0, fail qualification with the raw status preserved in
permanent fault metadata and the final JSON (`fault_exit_status`). This is a
Bash-status check, not an exact kernel wait-cause attestation: explicit `exit
137` and termination by SIGKILL can both produce 137. The waited PID handle and
its pidfile are cleared before any failure cleanup can signal it again.

Only current logical requests and one concurrency-bounded fault snapshot are
retained. After requests complete, qualification requires a witness with
`sent <= signal_before` and `finished >= signal_after`. Completion is timestamped
immediately after the client operation, before any tracking lock; the completion
lock is never held across the signal. A request that finishes after the signal
but before endpoint validation can qualify, while one already finished before
the signal cannot. There are no artificial runtime response holds, repeat fault
calls, or retries to manufacture a witness. Same-ID retries remain one logical
operation. Missing overlap fails as **not qualified**, distinct from the request
availability gate. This client-observed interval does not prove that the failed
voter was processing the witness. Healthy lanes disable this control/trace path.
Earlier n1 signal/request ordering remains unknown; deterministic counterexamples
to the old two-probe oracle do not establish the cause of that historical run.
Both server revisions use the same candidate client and record its revision,
binary SHA, receipt oracle, and instrumentation mode. Receipt decoding and
fault-only trace/control overhead are included; do not compare these results
as though they used the old uninstrumented client. A final query alone does not
prove before-ACK object-store durability.

`bash benchmarks/run-server-reference_test.sh` checks the real runner wiring
with explicit stand-ins, including early fault failure and owned-child cleanup.
These mocks are not an actual server or fault qualification. Go client tests
separately exercise receipt rejection, actual local HTTP trace lifecycle and,
on Linux, disposable-child pidfd signaling and birth-identity refusal.
The runner terminates and reaps its exact owned benchmark child on early exit,
while preserving the original failure status.

Hiqlite remains an external Raft reference, not a direct algorithm comparison.
Its leader/follower cases gracefully stop one peer and wait for a replacement
leader before measurement, so they describe post-failover steady state rather
than failover interruption. The pinned source patch is kept in
`benchmarks/hiqlite-one-peer.patch`; CI verifies its recorded digest before
reusing the result.

The workflow is advisory: candidate benchmark failures fail the job, while a
measured regression is reported without an arbitrary threshold. A failing
baseline invocation is retained as raw evidence, including its original exit,
but none of its measurements enter the statistical comparison. The required
candidate names and subcases are tracked in `ci-benchmark-matrix.txt`; every
required case must have the configured sample count. Missing, extra, or malformed
candidate measurements fail qualification. Older baseline-only or incomplete
cases are reported in `unavailable.txt` and excluded from comparison input;
complete common cases remain eligible. Retaining an unavailable baseline does
not certify a complete paired result. Use `workflow_dispatch` to compare the
selected commit
against another base ref. The same collector can be smoke-tested locally with
short samples:

```bash
RHIZA_BENCH_COUNT=1 RHIZA_BENCH_TIME=100ms \
  benchmarks/run-ci-benchmarks.sh HEAD HEAD /tmp/rhiza-performance
```

The collector's bounded mock regressions run with
`sh benchmarks/run-ci-benchmarks_test.sh`. They check output/exit handling,
matrix completeness and baseline isolation, not actual Go benchmark execution
or the performance of any workload. The opt-in Issue #185 job separately rejects
an exit-zero `go test -run` invocation without a passing test record.

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

## Opt-in E2E write-cost matrix

Dispatch `Performance` from a branch with the matrix harness, using full commit
SHAs for the two server revisions:

```bash
gh workflow run performance.yml --ref "$HARNESS_REF" \
  -f e2e_cost_matrix=true -f e2e_cost_provider=s3 \
  -f base_ref="$BASE_SHA" -f candidate_ref="$CANDIDATE_SHA"
```

This runs instead of the ordinary Performance suite. On one `ubuntu-24.04`
runner, the same client tests SQL, Graph, and KV against three healthy QUIC voters
and a disposable VersityGW S3 backend. Select `e2e_cost_provider=gcs` for the same
matrix against a pinned Floci GCS instance on loopback with synthetic credentials.
For each workload and durability mode,
base and candidate run serially in alternating order across three repeats
(36 samples total). Each sample requires 1,000 successful writes at concurrency
16 and a workload-specific final state check. This measures normal HA writes,
not Local mode, managed Kubernetes recovery, or fault tolerance.

- `result` reports write throughput and latency, including p99. `voter_resources`
  records per-voter user/system CPU tick deltas and RSS samples bracketing the
  timed write client. The CPU window excludes setup, final verification, and
  forced publication; no client or gateway resources are sampled. `VmHWM` is
  each voter's lifetime peak up to the sample and can include startup; summing
  those peaks does not produce a simultaneous cluster peak.
- `write_object_store_delta` covers setup complete through the writes;
  `object_store_delta` also includes final verification. BeforeAck waits for
  object-store durability before returning success. Async acknowledges quorum
  WAL durability first. After Async verification, `async_publication` separately
  measures a forced archive call and requires the archive to advance through
  the pre-call certified/applied cut. This is not an independent restore test
  or a measurement of scheduled background publication overhead.
- Object-store HTTP counters are client transport attempts. GCS results also
  classify Floci server access records into Class A and B attempts, separate
  resumable transfers/probes, and retain SDK and server count differences.
  Attempts include failures, and multiple HTTP requests can represent one
  billable operation. Versity does not measure GCS classes. See
  [GCS operation pricing](https://cloud.google.com/storage/pricing#operations-pricing).

The `e2e-cost-matrix-<run-id>` artifact records both SHAs, the harness SHA,
runner metadata, raw result JSON, process logs, and exit statuses. The matrix
stops on the first failure and uploads available evidence; a partial matrix is
not a passing comparison.

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

The run selects `TestVersityGatewayCostEvidence`, `TestVersitySDKRetryEvidence`,
the focused `TestVersityConditionalLargePutEvidence` and tagged
`TestArchiveExtentAttributionActualGuard`, followed by `TestCheckpointCost`,
`TestArchiveGCCost`, and
`TestCheckpointRestoreCostExercisesNodeRecoveryPathS3`. It then runs the
build-tagged `TestNodeOpenCatchUpRepairsHeldLearnGapForConcurrentKVCalls`
lifecycle regression (normal and `-race` variants) and three serial
`TestNodeForegroundAPICostS3` scenarios: baseline, `checkpoint-active`, and
`archive-cleanup-active`. These are ten core provider/checkpoint/GC/restore/Node
selectors plus two focused regressions; the focused regressions are not new
original-#185 gates. The actual foreground
acceptance result is the three-Node Before-ACK Node test; the bare-core
`pkg/network` component matrix is not in this job. Each Node foreground
scenario retains its 30-second warm-up and 120-second measured window.
Node foreground commands use a seven-minute outer test deadline: the approved
two-minute preparation budget, followed by the unchanged post-seed warm-up,
measurement, drain, and cleanup reserve (4m42s) and an 18-second margin. This
changes neither the measurement window nor any API, durability, checkpoint,
teardown, or GC guarantee.
Only `archive-cleanup-active` establishes a real certified checkpoint, matching
core seal, and fresh shared CURRENT before warm-up, within that same two-minute
preparation budget. Its setup I/O is accounted separately; this precondition is
not GC-work evidence and does not weaken the post-cleanup certified
seal/shared-CURRENT guard.
The restore case publishes a certified checkpoint through one node, removes only
the local SQLite files, then verifies that a second `Node.Open` recovers the
value and reaches ready.

The checkpoint cases report `snapshot_scan_hash_ms_by_role` separately from
end-to-end creation time. This opt-in measurement covers each source's block
read/hash loop and ends before its block uploads; it is wall time including
source I/O and scheduling, not hash CPU time. Failed scans do not produce a
successful zero-duration record. Ordinary `CreateFiles` calls do not enable
the timing observer.

The held-gap regression uses async object-store durability to isolate automatic
Node catch-up and ACK-after-apply behavior; it is mechanism evidence, not a
foreground cost result. It runs in both normal and `-race` variants; the
`-race` variant is a supplementary data-race check, not a separate correctness
claim. `TestNodeForegroundAPICostS3` uses the production
Before-ACK API path through three opened Nodes and preserves the same 4096-key,
16-worker, 30-second warm-up, 120-second measurement, and 30-second call bounds.
All three scenarios use that workload. Baseline has no maintenance worker;
`checkpoint-active` repeatedly exercises archive sync and certified checkpoint
publication/readback; `archive-cleanup-active` repeatedly invokes the archive
manager's cleanup path with the explicit 24-hour GC grace configuration. The
latter is archive-cleanup evidence, not a claim that the full scheduled Node GC
path ran. Maintenance is serial and bounded to the existing 10-second
preparation context; it stops scheduling at the measured-window cutoff and
joins before Node teardown. The report separates maintenance overlap from
non-overlap API PUT/GET cohorts and reports all-completion and successful p99
only when each cohort has at least 10,000 samples. Store-counter deltas during
maintenance overlap include concurrent API traffic, so they are interval totals
and not attributed exclusively to maintenance. These selectors also report
per-phase logical API counters, per-Node object-store deltas, and independent
Versity access-log request counts. The build-tagged Node runs are opt-in within
the explicitly dispatched evidence job and require `rhiza_local_testhooks`.
The tagged Node runs also retain only per-phase transport-error aggregates and
the first sanitized failure detail (request method/status family, error class,
and elapsed time), including checkpoint cleanup preparation and each sequential
Node shutdown; phase is sampled at request start. It does not log URLs, keys,
headers, credentials, or bodies. This is diagnostic attribution, not a claim
that retry metadata is available.
The two focused regressions preserve physical errors and unknown retry metadata
without imposing a universal zero-error or fixed-attempt gate: same-operation
final result, guard outcome, and exact readback remain mandatory for safe
attribution. They are focused regressions, not additional original-#185 gates.
Optional `pkg/network` component diagnostics remain available for focused local
investigation, but they are outside the required Issue #185 evidence job and are
not Node API foreground measurements. For example, one baseline component run
can be requested directly with:

```sh
RHIZA_FOREGROUND_API_COST_SCENARIO=baseline \
RHIZA_FOREGROUND_API_COST_REPETITION=1 \
go test -count=1 -v -timeout=5m ./pkg/network -run '^TestForegroundAPICost$'
```

The `checkpoint-active` and `gc-active` scenario values remain supported; a
repetition value of `1`, `2`, or `3` selects the labeled repetition. These
component results must not be substituted for the actual Node API foreground
p99, nor required to pass as a condition for the provider/Node acceptance
artifact. The three-Node Before-ACK result is the actual Node API measurement.

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
meaning. `observed_request_identities` and `observed_request_repeats` group
same-identity `RoundTrip` calls only within one metered logical bucket
operation. This supplemental observer is disabled by default on `NewBucket`;
the cost fixtures explicitly mark the context passed to `NewBucketWithContext`
or `Node.Open`. `replay_grouping_enabled` distinguishes disabled counters from
an observed zero-repeat result. Identity is a private digest of method, URL
components, declared length, and selected range/conditional headers; request
bodies are not compared. A repeat is an observed repeat, not proof of an SDK
retry; changed request identity, absent context, capacity loss, or an open reader at snapshot
can leave grouping incomplete. `request_grouping_unknown` counts observed
RoundTrip calls that could not be grouped; capacity/incomplete counters and
active-operation/open-reader gauges expose tracker limitations. Retries made
below the wrapped RoundTripper are invisible to these counters and require
independent server records to observe. These measures do not replace MinIO/AWS
retry metadata, which remains separately known or unknown. The local Versity
results are local S3 compatibility evidence only—not AWS/GCS qualification or a
release gate.

`go test ./internal/objstore -run '^$' -bench '^BenchmarkReplayGroupingOverhead$'`
compares the metered transport with and without one tracked logical operation
using a synthetic RoundTrip. On the recorded Apple M1 run, tracking measured
846.7 ns/op and 984 B/op (10 allocs), versus 175.6 ns/op and 248 B/op
(5 allocs) for the disabled transport-only case. This microbenchmark isolates
the added grouping work only approximately; it is not an end-to-end performance
result.

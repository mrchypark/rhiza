# Local archive GC comparison

Baseline: `bc944c1521f588feee9c5f5a59abe37d15f8063d`; candidate: `38af008dd99f1073b682a224ae01f0a4c83c1f30`. The baseline uses unchanged production source plus the identical opt-in measurement fixture. Candidate source and test-binary hashes are in environment.json.

Only eligible object/marker delete pairs run concurrently, capped at four. Classification, grace, generation checks, recovery pins and GC lease coordination are unchanged. Each marker is deleted only after its object succeeds or is already absent; all workers join before lease release. The existing x/sync dependency is now direct.

## Final measurements

Three alternating pairs (base/candidate, candidate/base, base/candidate), fresh prefix per run, 1 KiB orphan objects plus one reachable extent. Durations are median milliseconds for the entire Cleanup call, including lease, head, pin and listing operations. Setup, verification and fixture removal are excluded. Zero grace is used only in the fixture to separate marking from deletion without waiting.

| Orphan objects | Phase | Serial ms | Concurrency 4 ms | HTTP requests, both |
|---:|---|---:|---:|---|
| 32 | mark | 93.971 | 54.415 | [44] |
| 32 | delete | 68.835 | 25.553 | [77] |
| 256 | mark | 371.418 | 465.183 | [268] |
| 256 | delete | 324.233 | 105.307 | [525] |

The 256-object delete median fell from 324.233 ms to 105.307 ms (67.5%); all three final paired delete samples improved. Marking remains serial and varied independently, so this is not a claim of a total-GC or application throughput improvement. The exploratory runs also improved deletion but showed substantial host/provider noise; their raw logs are retained, not discarded.

For these successful short runs, marking uses N+12 HTTP requests (N+2 PUT, 6 GET, 4 HEAD), and deletion uses 2N+13 (2N DELETE, 2 PUT, 7 GET, 4 HEAD). LIST HTTP calls are counted as GET. Both variants have zero SDK retries, transport failures, HTTP failures, unexpected 4xx and 5xx. The first marking pass records one logical failure from an expected missing GC-lock read; it is not an HTTP failure. No renewal was needed within these short runs.

API counts did not decrease. This is a bounded latency optimization, not an object-store billing reduction. No cloud-provider latency, CPU/RSS, foreground workload interference or production scale claim is made. Health-request timings in environment.json describe the local HTTP path, not S3 operation latency. Four is the delete-pair limit, not a global HTTP concurrency limit.

## Reproduction

Run a disposable S3-compatible bucket and supply `RHIZA_GC_BENCH_ENDPOINT`, `RHIZA_GC_BENCH_BUCKET`, `RHIZA_GC_BENCH_ACCESS_KEY`, and `RHIZA_GC_BENCH_SECRET_KEY`. The endpoint uses HTTP; use local test credentials only.

```sh
go test ./pkg/recovery -run '^TestArchiveGCCost$' -count=3 -v
```

The fixture owns/removes a unique prefix and verifies live tip preservation and orphan/marker counts. Normal tests skip this measurement without an endpoint. Safety coverage includes bounded concurrency, object-before-marker order, failure marker retention, joined errors, cancellation of queued work and waiting for outstanding calls before lease release; existing grace/pin/future-generation/stale-collector tests remain in the package.

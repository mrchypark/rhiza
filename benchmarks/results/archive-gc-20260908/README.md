# Archive GC request-cost check

Revision `eb94a0d588a409b114a06bfbe2ee78b4f8143279`, run locally on 2026-09-08 against a disposable Docker MinIO bucket. No standalone network RTT was measured; timings include the local Docker/provider path. This confirms the existing #118 implementation's request shape; it is not a cloud-provider cost or throughput claim.

| Orphans | Phase | HTTP requests | DELETE requests | Duration (ms) |
| ---: | --- | ---: | ---: | ---: |
| 32 | mark | 44 | 0 | 135.567 |
| 32 | delete | 77 | 64 | 28.994 |
| 256 | mark | 268 | 0 | 745.463 |
| 256 | delete | 525 | 512 | 223.363 |

For a successful deletion pass, the count remains `2N + 13` requests: one delete for each unreachable immutable object and one only-after-success delete for its durable GC marker, plus the fixed lease/head/pin/listing operations. The generic `objstore.Bucket` API only exposes single-object `Delete`; it has no conditional or provider-neutral multi-delete operation. Replacing that pair with a provider-specific S3 batch call would change the failure and object-before-marker guarantees, so this result does not justify a production code change.

All measured runs had zero SDK retries, transport failures, HTTP failures, unexpected 4xx, and 5xx responses. The mark pass's one logical failure is the expected initial missing GC-lock probe. Grace is zero only to make the mark and delete passes deterministic in this disposable fixture; recovery pins, generations, CAS publication, and lease behavior remain covered by package tests.

The 2026-09-06 local MinIO results had the same request counts (44/77 at 32 objects and 268/525 at 256). This single 2026-09-08 sample confirms the count, not a latency improvement.

## Reproduce

Run a disposable S3-compatible bucket, create the named bucket, then run:

```sh
RHIZA_GC_BENCH_ENDPOINT=127.0.0.1:19000 RHIZA_GC_BENCH_BUCKET=rhiza-gc-cost-20260908 RHIZA_GC_BENCH_ACCESS_KEY=rhiza-test RHIZA_GC_BENCH_SECRET_KEY=rhiza-test-secret go test ./pkg/recovery -run '^TestArchiveGCCost$' -count=1 -v
```

`RHIZA_GC_BENCH_ENDPOINT` is a host and port, without `http://`; the test itself selects insecure HTTP. A cloud-bucket decision still needs an explicitly authorized representative bucket, retention/versioning policy, object count, grace period, and provider billing data.

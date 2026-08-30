# Final 13 archive batching validation

## Result

The final 13 change expands an already-open `before-ack` archive batch to the
current contiguous certified tip. It does not wait longer or publish
uncertified values. All SQL, Graph, KV, fault, checkpoint, and recovery requests
completed without client errors.

## Healthy before-ack A/B

The Graph A/B was run in both orders and contains six samples per workload.
The SQL A/B contains three samples per workload. Checkpoints were disabled and
the background archive interval was 10 minutes so the measured object calls
belong to synchronous before-ACK publication.

| Workload | final 12 HTTP calls / 100 writes | final 13 | Change | p50 final 12 | p50 final 13 |
|---|---:|---:|---:|---:|---:|
| Graph write | 91.7 | 70.8 | -22.7% | 20.41 ms | 23.69 ms |
| Graph KV write | 90.8 | 63.3 | -30.3% | 22.01 ms | 17.91 ms |
| SQL write | 76.7 | 70.0 | -8.7% | 19.64 ms | 17.95 ms |
| SQL KV write | 93.3 | 76.7 | -17.9% | 20.26 ms | 18.58 ms |

Graph write latency was scheduler-sensitive: the reverse-order run favored
final 13 while the forward run contained a slow final-13 sample. Across both
orders Graph cluster CPU was flat (396.3 to 395.1 microseconds/request), summed
memory.current fell 1.3%, and summed memory.peak fell 0.5%. SQL CPU fell from
206.2 to 179.0 microseconds/request; summed memory.current and memory.peak fell
20.4% and 18.0%. These are short single-host Dory measurements, not capacity
claims.

## Fault, checkpoint, and recovery

- One peer failed: SQL and Graph before-ACK suites served 25,200 requests with
  zero errors. Quorum became writable in 98 ms (SQL) and 123 ms (Graph).
- One-second checkpoints: SQL and Graph before-ACK suites served another
  25,200 requests with zero client errors and zero unexpected S3 4xx/5xx or
  transport failures. Conditional CAS conflicts are expected contention.
- Two peers failed: both APIs returned HTTP 503 `commit_unknown`; retrying the
  same request ID after quorum recovery committed exactly once.
- Deleting and recreating all three `emptyDir` pods restored SQL and Graph from
  shared object storage and served a linearizable query containing all writes.

## Verification

- `GOEXPERIMENT=arenas,greenteagc go test -count=1 ./...`
- `GOEXPERIMENT=arenas,greenteagc go test -tags graph -count=1 ./...`
- focused default and Graph race suites
- default and Graph `go vet`
- `git diff --check`

Raw NDJSON, cgroup CPU/memory snapshots, object-store counters, fault timing,
and chaos/recovery logs are stored beside this report.

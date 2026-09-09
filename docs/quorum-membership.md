# Quorum-controlled membership (experimental)

This work adds a **QuePaxa protocol prototype**, not automatic recovery for
`rhiza.Open`, the Kubernetes Operator, or no-PVC deployments. Those hosts still
use fixed membership and retain their lost-WAL protection. Do not enable this
prototype on a production cluster.

## Protocol boundary

Normal operation keeps the 16-slot pipeline. In reconfiguration mode a recorder
also refuses a slot more than 16 positions ahead of its contiguous certified
tip. This is bounded lookahead, not sequential consensus. Lagging recorders
may exert backpressure; throughput still needs qualification.

A membership change proceeds under the existing configuration:

1. Commit a freeze at slot `F`. The first freeze in the contiguous prefix wins.
2. Reject ordinary proposals while resolving the old configuration's remaining
   slots through `F+15`. Recovery requests name the exact freeze operation.
3. Commit the terminal transition at the deterministic slot `F+16`, binding the
   target configuration and the resolved prefix.
4. Use the new configuration from `F+17` onward. Its members and leader order
   apply only to its own slots; historical certificates retain their original
   configuration.

Freeze dissemination waits for the recorders named by its certificate.
Terminal dissemination waits for both the old and new quorums, plus an explicit
durable acknowledgement from each added voter. Retrying `FinishReconfiguration`
retries terminal dissemination if the decision already exists.

The terminal disk barrier also persists the preceding decision prefix. A member
cannot vote in the new generation while that barrier is pending. Leader timing
collection and adaptive schedules restart for the new configuration.

A recorder that has not learned `F` has a tip at most `F-1`, so it cannot vote
at `F+16`. One that has learned `F` must apply the freeze before voting again.
This prevents the old pipeline from crossing the transition boundary. Freeze
quorums must explicitly acknowledge support for the bounded protocol; an old
binary's ordinary receipt is insufficient.

The bounded-lookahead rationale is related to the `WINDOW` rule in
[Paxos Made Moderately Complex](https://paxos.systems/how/). This implementation
adds a drain and terminal boundary; that source is not a verification of this
QuePaxa implementation.

The bounded promise must survive restarts: a WAL that has acknowledged this
protocol must not reopen as an unbounded voter. Terminal consensus must also
exclude responses whose old terminal-slot state has not been proven safe;
filtering only the final certificate is too late for QuePaxa candidate selection.
Sparse freezes are buffered until they enter the contiguous prefix. A later
freeze in the drain keeps its original bytes and prefix contribution, but does
not start a second transition.

## Lost voter example

With voters A/B/C and C's WAL permanently lost, A/B can authorize removal of C.
The resulting A/B configuration requires both surviving voters. A replacement
uses a new voter ID and token, starts with `quepaxa.NewLearner`, and receives
certified history without voting. A/B authorize its addition only after an
admission callback verifies its durable prefix at the transition boundary.
The target member must include that learner's `Core.WALIdentity()`. Reopening
the same WAL preserves this identity; an empty replacement WAL receives a new
identity and cannot reuse the previous incarnation's promotion.

`network.Transport.VerifyLearner` checks the WAL identity and prefix through the candidate's
token-pinned QUIC endpoint. Readiness, Pod identity, or a reported log index
alone is not admission evidence. Removed voter IDs must not be reused.

The core APIs are `BeginReconfiguration`, `FinishReconfiguration`,
`CurrentCluster`, and `ClusterForSlot`. Enable the prototype through
`quepaxa.Config.EnableReconfiguration`. Bind a network transport to its core
with `BindCore` before starting traffic. A plain `NewObserver` remains a
nonvoting verifier; it must never acquire voting authority by replaying a
membership record.

The QUIC transport pulls certified decision pages when a recorder reaches its
lookahead limit. Ordinary pages can be installed as hints; pages containing a
membership control require durable installation. A custom `quepaxa.Transport`
must provide equivalent prefix catch-up to keep certified-only writes moving.
Gap recovery has its own single-operation gate, so it can run while all sixteen
frontend pipeline positions are occupied.

## Current limits and deployment gate

- This prototype retains the complete WAL. Checkpoint creation, compaction,
  and checkpoint restoration in reconfiguration mode must fail closed until
  checkpoint certificates also bind configuration history and retirement
  state. An unbounded retained WAL is not an acceptable no-PVC recovery model.
- Archive publication, before-ack durability, GC authority, and remote voter
  registration still use fixed-generation contracts. They require certified
  configuration history and epoch fencing before Node can enable this feature.
- Learner provisioning, retry orchestration, discovery after losing a local
  identity, and Go/Rust/Operator integration are not provided by the core API.
- Quorum-controlled removal fences consensus participation. It does not kill
  the previous process or revoke its object-store credentials.
- Loss of the existing quorum still requires the separate generation recovery
  procedure. Membership changes cannot manufacture the missing quorum.

The feature must remain disabled in the production Node until these gates and
the crash/concurrency qualification are complete. A passing protocol test is
not a claim that automatic no-PVC recovery is ready.

## Local performance qualification

On Apple M3 / Go 1.27, the same candidate's parallel three-peer `Propose`
benchmark (1,000 operations, three runs) produced these medians:

| Mode | Time per operation | Allocations per operation |
| --- | ---: | ---: |
| Fixed membership | 13.49 ms | 296 |
| Reconfiguration enabled | 14.32 ms | 315 |

The pipeline remains 16 slots; this measurement shows approximately 6.2%
overhead, not identical throughput. It uses the in-process test transport and
local WALs, not a Kubernetes deployment or an end-to-end SQL workload. The
certified-only API also requires recorder catch-up and has a separate benchmark.
Production throughput and tail latency still require qualification.

A subsequent allocation optimization keeps internal configuration snapshots
read-only instead of copying the member slice on each lookup, while public
accessors still return copies. Voter checks scan the member slice directly.
With reconfiguration enabled, consecutive before/after batches on the same
machine (1,000 operations, three runs each) measured:

| Candidate | Median time/op | Median bytes/op | Median allocations/op |
| --- | ---: | ---: | ---: |
| Before snapshot optimization | 14.98 ms | 59,055 | 316 |
| After snapshot optimization | 14.27 ms | 54,689 | 300 |

This reduced allocated bytes by 7.4% and allocations by 5.1%. Timing varied
(before: 20.30 / 14.14 / 14.98 ms; after: 14.41 / 14.23 / 14.27 ms), so the
4.7% median time reduction is provisional, not a production throughput claim.
The earlier fixed-membership comparison above is a separate measurement.
Quorum validation, durable membership barriers, and pipeline width are unchanged.

Reproduce with:

```sh
go test ./pkg/quepaxa -run '^$' -bench '^BenchmarkCoreProposeThreePeersParallel' -benchtime=1000x -count=3
```

### WAL metadata reuse

WAL decision appends now use the already validated `DecidedValue` slot/hash,
removing four certificate decodes used only to reconstruct that metadata.
External certificate validation and WAL encoding remain unchanged.

A subsequent before/after batch (same benchmark, 1,000 operations, three runs)
measured median allocations of 299 -> 240 per operation and allocated bytes
of 54,663 -> 42,971 (21.4% less). Median time was 14.18 -> 14.55 ms;
this run does not establish a latency improvement.
Before times: 12.99 / 14.18 / 14.40 ms. After: 13.94 / 14.59 / 14.55 ms.

Remaining performance work is tracked in [the optimization plan](performance-optimization-plan.md).

### Network optimizations 2–4

Bound transports now resolve `Core.ConfigIDForSlot` / `Core.ConfigID` before
consulting their cache. A hit copies no members; a miss obtains a public
snapshot and caches under its actual ID, even if the configuration advanced
between lookups. Each configuration still owns its credentials, connection
pools, and TLS session cache. Public cluster accessors continue returning copies.

Peer authentication directly scans the historical and current member slices
for the sender, preserving last-entry lookup semantics. Both membership checks,
constant-time token comparisons, removed-voter fencing, and the restricted
learner administrator exception remain intact. Actual QUIC membership tests
exercise historical sync by a surviving voter and rejection of a removed voter.

Catch-up serializes callers per source before taking either of the two global
fetch slots. Waiters recheck their own target and keep their own cancellation
context. Another source can use the second slot. Durable completion calls
`EnsureDurableThrough`: existing hints are appended and covered by one shared
sync before being marked durable. A raised in-memory Tip is insufficient.
Membership control pages still force durable installation, including the
terminal prefix barrier; the control-page test reopens the WAL in configuration 2.

The helper reuses checkpoint prefix appends, checks concurrent recovery-base
advancement under the slot and Core locks, and never recreates compacted state.
WAL rollover syncs the old segment, and compaction preserves post-fence tails.
Group commit detaches a batch before starting its sync, so new appends cannot
join a completed earlier barrier. Failure/retry, one-sync hint promotion,
cancellation, and a real recovery-base installation racing with sync are tested.
Cancellation does not undo prior appends or stop an already running shared fsync.

The deliberate limit is an O(retained prefix length) durability scan. A durable
watermark is deferred until repeated scans of long WALs justify it. Different
sources can still fetch overlapping pages; the limit remains two, and the normal
pipeline remains sixteen. These changes do not complete any deployment gate above.

Measurement for steps 2–4 will use the existing Performance CI on
`ubuntu-24.04`, with `GOMAXPROCS=2`, ten alternating samples per revision,
and the focused `transport`, `auth`, and `catchup` modes. CI dispatch is pending:
the candidate and workflow additions are uncommitted, and publishing the
comparison snapshots requires extending the earlier no-commit/no-push scope.
No valid performance result for these three steps is available yet. A local
sample that overlapped another task's tests was discarded. The full Go tests,
race detector, vet, CLI/Operator build, and CI configuration lint passed.
See [the optimization plan](performance-optimization-plan.md) for exact workloads
and the sequential revision comparisons. Allocation-free cache hits and a
single fetch in the concurrency test are correctness checks, not CI latency results.

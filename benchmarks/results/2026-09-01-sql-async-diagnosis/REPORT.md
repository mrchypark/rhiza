# SQL async throughput diagnosis

> Resolved by installing the remote quorum-certified decision before invoking
> catch-up. See `../2026-09-01-sql-async-fix/REPORT.md` for the fix and repeated
> verification. The original missing-backpressure explanation below correctly
> described the overload mechanism but did not identify this earlier ordering
> bug as its trigger.

## Verdict

The unexpectedly low SQL async result is reproducible and is not object-store
latency. Under a 5,000-write steady-state probe, async collapsed to **14.8
successful writes/s** and returned **993 HTTP 503s**. The matching before-ACK
probe completed **5,000/5,000** writes at **134.7 writes/s**.

During the async probe, all three voters repeatedly logged decision-sync
failures (`context deadline exceeded`, `deadline exceeded`, and QUIC
`APPLICATION_ERROR`). CPU stayed below 0.1 core per pod. The failure is therefore
in the foreground proposer/catch-up path, not CPU or foreground object-store
work.

The code path explains the inversion:

1. Requests enter through `rhiza-sql-1`, while the agreed proposer may be a
   remote voter.
2. A remote proposal response can be ahead of the local tip, so
   `acceptFrom` calls `catchUpFrom`.
3. Catch-up is limited to two concurrent operations and each decision page has
   a 250 ms timeout.
4. Unthrottled async proposals outrun that catch-up path, causing sync retries
   and quorum/commit-unknown HTTP 503 responses.
5. before-ACK's durability barrier unintentionally supplies backpressure, so
   the local node remains close enough to the proposer to avoid the collapse.

The causal step from observed sync failures to missing backpressure is an
inference from the code and measurements; per-slot batch/catch-up telemetry is
not currently exposed.

## Measurements

All probes used the current `rhiza-e2e:latticedb-0.2.1-current` image, three
fresh `emptyDir` voters, concurrency 16, a 500-write warm-up, checkpointing
disabled, and a 10-minute async object-store interval.

| Mode | Requests | Success | HTTP 503 | total ops/s | success ops/s | p50 | p95 |
|---|---:|---:|---:|---:|---:|---:|---:|
| async | 5,000 | 4,007 | 993 | 18.4 | 14.8 | 338.2 ms | 2,410.0 ms |
| before-ACK | 5,000 | 5,000 | 0 | 134.7 | 134.7 | 97.7 ms | 256.4 ms |

The existing 100-write runner is not a reliable capacity test. Its isolated
primary medians were 51.2 ops/s async and 223.5 ops/s before-ACK, while the
immediately following resource pass reversed direction at 74.5 and 31.2 ops/s.

## Evidence

- `raw/isolated-sql-{async,before-ack}.ndjson`: isolated 100-write primary runs
- `resources/*-resource.ndjson`: independent 100-write resource runs
- `long/{async,before-ack}-warmup.json`: 500-write warm-ups
- `long/{async,before-ack}-5000.json`: steady-state probes

No production code was changed during this diagnosis. The Dory SQL StatefulSet
was left healthy in `before-ack` mode after the probes.

# Cache expiry

Rhiza's typed KV values can have a TTL. A read hides a value once its expiry is
at or before the read time. Physical rows are removed only while applying a new
KV command: its replicated timestamp selects expired rows, and it prunes at
most 256 before performing the requested KV operation. An idle node does not run a background collector.

Application SQL tables remain application-owned. A table name containing
`_rhiza_` is reserved by Rhiza's public SQL API. The following example uses
`app_cache` with a `(namespace, cache_key)` primary key, a binary `value`, and
nullable `expires_at` in Unix seconds. Cleanup is a bounded replicated mutation
with a cutoff chosen by the caller:

```sql
DELETE FROM app_cache
WHERE rowid IN (
  SELECT rowid
  FROM app_cache
  WHERE expires_at <= ?
  ORDER BY expires_at, namespace, cache_key
  LIMIT ?
);
```

Use one fixed caller-supplied cutoff for a cleanup pass. Validate the batch
limit in the host before submitting it: it must be an integer from 1 through
256. SQLite treats a negative `LIMIT` as unbounded. Add `CREATE INDEX ... ON
app_cache(expires_at, namespace, cache_key)` when creating the table. The
ordered subquery gives every replica the same bounded row set; the limit bounds
one consensus mutation. The retained expired-row count after a pass is
observable with:

```sql
SELECT COUNT(*) FROM app_cache WHERE expires_at <= ?;
```

The work is one replicated SQL mutation per batch. With a fixed snapshot of
`N` expired rows and no concurrent cache writes or retries, this takes at most
`ceil(N / limit)` cleanup commands; run a final empty pass because concurrent
writes can add rows while cleaning. Reuse the exact request ID, SQL, and
arguments when retrying an uncertain cleanup result. Use a new request ID only
for the next batch. `NULL` expiry values remain permanent. The public API test
below verifies a two-row batch, retry, permanent row, and final batch without
clock sleeps:

```sh
go test ./... -run '^TestApplicationCacheCleanupIsBoundedAndIdempotent$'
```

The benchmark measures one 256-row replicated cleanup after setup, with 256
and 4096 expired rows. Setup and retained-row count checks are outside the
timed interval:

```sh
go test . -run '^$' -bench '^BenchmarkApplicationCacheCleanup$' -benchtime=1x -count=3
```

Local evidence only, not a production performance claim: macOS 26.3 (25D125),
Apple M3 / darwin-arm64, Go 1.27.0; Rhiza baseline
`eb94a0d588a409b114a06bfbe2ee78b4f8143279`; measured benchmark fixture SHA-256
`cf2a912211e1489bb93f02899548a22a2d545d2867284aee26b7d2954803be70`.
The recorded fixture predates the example-table rename to `app_cache`; the
benchmark itself uses `cache_benchmark` and is unchanged. With `-benchtime=1x -count=3`, timed cleanup samples were 46.704ms, 105.467ms,
and 57.977ms for 256 expired rows (0 retained), and 36.899ms, 46.538ms, and
59.164ms for 4096 expired rows (3840 retained). The fixture creates the expiry
index and seeds outside the timed interval.

The caller controls when to run cleanup; Rhiza does not install an application
scheduler. Cache reads must continue to apply their own expiry predicate until
cleanup has caught up. KV expiry uses milliseconds; this SQL example uses seconds.

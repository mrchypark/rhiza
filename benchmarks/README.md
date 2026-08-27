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
  sql rhiza-sql-kv-e2e:dev async current-sql-async benchmarks/results/<run>
```

Set `RHIZA_BENCH_CHECKPOINT_INTERVAL=1s` to measure checkpoint interference. Add
`one-fault` as the sixth argument to measure with one peer unavailable. The runner
uses a fresh object prefix and empty pod volumes for every invocation.

Aggregate all NDJSON files with:

```bash
jq -s -f benchmarks/summarize.jq benchmarks/results/<run>/raw/*.ndjson \
  > benchmarks/results/<run>/summary.json
```

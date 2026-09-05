# Excluded measurement attempts

No samples from these directories are included in the final comparison.

- `initial-failed/`: v0.5 sample 3 failed while the pre-existing snapshot benchmark walked the live database directory to measure size. WAL rotation renames `wal.log` to `wal.base` before creating the next `wal.log`; the directory walk raced with that rename and got ENOENT. Replaced live directory traversal with the size of an immutable `Snapshot.Backup` artifact, outside the timed section. This was a benchmark measurement defect, not evidence of missing committed data.
- `snapshot-contention-failed/`: after fixing the measurement defect, v0.5 sample 3 failed at initial `beginGraphSnapshot` with `write transaction is already active`. LatticeDB's background checkpoint uses the same writer mutex as the nonblocking `BeginSnapshot` API. Rhiza serializes its own graph writers but cannot exclude that internal worker. This requires handling the transient snapshot lock conflict in the shared snapshot entry point, respecting the caller's context.

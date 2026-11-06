# Rhiza v0.23.1

This patch prepares the Rust crate and current installation examples for
`rhizadb` 0.23.1. Its functional scope is test-fixture reliability, benchmark
measurement validity, and qualification documentation. It does not introduce
an SDK API change or a production-engine performance optimization.

## Changes

- Separate failed baseline benchmark output from successful statistical input
  while preserving raw output, diagnostics, and original exits. Validate the
  required candidate benchmark matrix and per-case sample counts.
- Validate SQL benchmark INSERT receipts and the exact final row identity set,
  rather than treating HTTP 2xx and aggregate cardinality as exact ACK proof.
  Use the same candidate client and oracle for both server revisions.
- Coordinate a single fault attempt with Linux pidfd identity binding and
  same-process monotonic request/signal intervals. Preserve the observed target
  child exit status and require Bash status 137 for fault qualification.
- Make benchmark-child and controlled test-fixture cleanup failure-safe, and
  strengthen conflict-winner effects, pruning postconditions, and actual
  certified-prefix synchronization assertions.
- Align manual Chaos documentation with the actual injection, runtime-turnover,
  fault-object absence, and application-recovery checks. Do not substitute an
  unverified controller-condition claim for those checks.

These changes strengthen evidence used for later optimization. They do not
claim faster production SQL, KV, or Graph operations, weaker durability rules,
or a newly implemented recovery/fencing mechanism. Historical failures and
their diagnostic evidence remain part of the record.

## Pre-version qualification

The following runs qualify the pre-version source revision
[`6395fd6888b2ac654976cbd8d3d79244622ae07e`](https://github.com/mrchypark/rhiza/commit/6395fd6888b2ac654976cbd8d3d79244622ae07e):

- [General CI 42163751772](https://github.com/mrchypark/rhiza/actions/runs/42163751772).
- [Performance 42163754813](https://github.com/mrchypark/rhiza/actions/runs/42163754813).

They are not evidence that the subsequent version/documentation commit, merged
main revision, or published 0.23.1 artifacts have already passed verification.
New-head CI and release-artifact verification remain pending until separately
completed. A release is not established by these version edits alone.

## Evidence limits

Fault overlap is a client-observed logical-request interval, not proof that the
failed voter was processing that request. Fault lanes require Linux pidfd
support and fail closed when the identity or interval cannot be established.
The earlier n1 signal/request ordering remains unknown; a deterministic
counterexample to the old two-probe oracle is not its historical root cause.

Bash wait status 137 cannot distinguish explicit `exit 137` from SIGKILL. It is
not an exact kernel wait-cause attestation. Receipt parsing and fault-only
instrumentation are part of the measured workload; results must retain their
client revision, binary hash, oracle, and instrumentation metadata. Final row
readback alone does not prove before-ACK object-store durability.

See the [benchmark methodology](../benchmarks/README.md) for the implemented
gates, workload boundaries, and comparator limitations. No universal regression
tolerance or performance-improvement claim is introduced by this patch.

# Independent terminal evidence retention

This implements the retention boundary required by the [original Pro review](reviews/2026-10-21-dead-pod-autohealing-pro.md)
for [issue #246](https://github.com/mrchypark/rhiza/issues/246). It is **not**
production dead-Pod healing qualification. The v2 attestation consumer, real
runtime provider, authenticated mutation routing, and live qualification remain
unfinished. No application-facing endpoint or default authorization is added.

`pkg/operator/terminal_evidence_store.go` retains one closed run's original stop
request, complete runtime enrollment, pinned mutation producer UID, exact store
scope, original runtime receipt bytes, and settled mutation snapshot. The snapshot
includes its immutable archive head; referenced archives and provider receipts
must remain retained. Verification cursors are not part of the terminal result.

The authority verifies evidence, creates an immutable content-addressed ConfigMap,
reads its server-assigned UID and contents back, and only then records the exact
terminal reference through the root ledger CAS. An uncertain create or root CAS
response is resolved by readback of that same proof/reference. Neither merely
creating an artifact nor finding its digest is an authorization.

On reuse, the loader checks the root reference, object UID, namespace, immutable
flag, deletion state, strict bounded JSON, content digest and every retained
binding. It invokes the trusted runtime verifier again. Prepared mutations need
the trusted provider receipt verifier; all prepared descriptors must name the
same store scope. Missing verifiers or invalid receipts fail closed. The runtime
verifier must certify the complete enrolled incarnation and original closure,
including descendants and prevention of return; this callback is not itself a
CRI implementation.

The producer's closed operation and frozen admitted interval cannot change.
Subsequent compaction is allowed, but the original archived head must be in the
current committed history, not an orphan or merely below a larger watermark.
Each verification identity checks the complete archive chain in persisted batches
of at most 32 records. An incomplete batch returns pending; executor restart
resumes it. A new generation uses a new verification identity rather than an old
completed cursor. The cursor is additionally bound to the exact terminal artifact.

The original voter stop binding survives later generation closure. Retention does
not set `Run.Exited`, synthesize a cooperative drain, clear the run, or make the
cooperative `Ledger.Complete` predicate true. A future v2 consumer must include
and verify every historical run, not skip runs lacking current Pod targets.

## Deterministic coverage

`TestTerminalStore*` covers:

- voter closure followed by generation closure, compaction and executor restart;
- immutable artifact and root-CAS response loss without duplicate evidence;
- unclosed runs, unsettled work, missing runtime verifier and rejected receipts;
- missing/replaced/mutable/deleting/corrupt artifacts, unknown fields and trailing JSON;
- wrong store and missing/rejecting verifiers after restart;
- 70 archive links verified across three restarted executions;
- a new generation detecting a missing ancestor despite an earlier completed scan;
- never-bound producer UID pinning and prepared mutation receipt enforcement;
- preserving `Excluded` as exclusion, not relabeling it committed success.

These tests use an in-process fake Kubernetes API and explicitly injected test
verifiers. They do not establish trusted CRI termination, provider settlement,
production historical proof composition, Kubernetes recovery, or GCS/Chaos PASS.

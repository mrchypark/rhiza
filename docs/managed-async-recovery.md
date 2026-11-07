# Managed Async recovery from a published cut

Status: decision record for a proposed recovery path. Managed Async and
archive-only write activation are not implemented or qualified.

## Decision and scope

Keep `before-ack` as the managed default and preserve its durability contract.
The proposed explicit Async mode would acknowledge a certified decision after
quorum WAL durability, without waiting for object-store publication. If the old
quorum and its WALs are lost, the approved loss boundary is the exact verified
published cut: acknowledged operations after that cut may be lost. This is not
permission to lose verified history at or below the cut, recover from unproved
data, or allow two generations to write. An Async ACK is not an archive receipt.

Today the [initializer](../pkg/operator/initializer_v1alpha2.go) and
[managed Host](../pkg/managedsql/host.go) reject explicit Async. The
[archive read-only view](../pkg/recovery/readonly_view.go) can verify a captured
head without activating a writer. The [generation-anchor verifier](../pkg/recovery/anchor.go)
rejects managed request policies 9 and 10, and the
[legacy recovery controller](../pkg/operator/controller.go) cannot run. These
components are evidence or building blocks, not an existing activation path.

## Recovery stages and refusal conditions

1. **Fence the old generation.** Obtain independent evidence covering the
   exact old runtime and storage identities: termination or effective isolation,
   prevention of recreation or resumed writes, and no outstanding storage work.
   Pod deletion, a timeout, an archive seal, and a fence digest alone do not
   provide that evidence. Unknown scope or unfinished work blocks recovery.
2. **Freeze and verify the published cut.** After fencing, fix the exact source
   binding, captured archive head bytes and digest, certified tip and prefix
   digest. Verify membership history, the checkpoint and its object hashes when
   applicable, and the contiguous certified suffix through that tip under the
   source policy. Missing or conflicting evidence blocks recovery; do not switch
   to a different head on retry or use a fresh genesis to bypass missing authority.
3. **Prepare the successor without exposing writes.** Restore the verified state
   and retained request evidence in the successor. For retained SQL, KV, and
   Graph originals inside the cut, preserve kind, raw request ID, fingerprint,
   canonical slot, original result and retry boundary. Recovery must not extend
   retention. Outside the cut, or when the original is missing or expired, do not
   infer non-execution and do not transparently replay an old request as new.
   The published cut defines data loss, not authorization to reissue an operation.
4. **Activate only with live authority and a ready quorum.** Bind the exact
   recovery request, verified cut and target to the live source binding and
   generation. Require the authority transition to succeed and verify its
   receipt against the live target binding and generation before exposing writes.
   A missing anchor, unresolved pending application write, conflicting transition,
   or stale receipt blocks activation. A ready successor quorum is also required;
   neither readiness nor a stored receipt alone authorizes writes. See the
   [anchor coordinator](../pkg/recoveryanchor/coordinator.go) for the existing
   distinction between transition receipts and live binding checks.

## Required qualification cases

| Case | Required observation |
| --- | --- |
| BeforeAck control | Existing managed defaults and durable ACK behavior are unchanged. |
| Async with delayed publication | A certified, WAL-durable ACK can be above the published cut; recovery after WAL loss restores exactly the verified cut and does not report the missing tail as durable or replay it. |
| Wrong or changing source | Wrong binding, digest, checkpoint, membership, policy, a gap, or changed captured head blocks activation without exposing a writer. |
| Old writer or outstanding work | An isolated old process, possible recreation, or unfinished storage write blocks activation; a fenced old writer cannot affect the successor. |
| Invalid or competing authority | Missing/stale anchor, pending write, conflicting transition, stale receipt, or concurrent activation cannot produce a second writer; no writable endpoint is exposed without a ready quorum. |
| Request continuity | A retained original returns the exact prior receipt with no new effect. An ID/payload conflict, missing tail ID, or expired original is never silently re-executed. |

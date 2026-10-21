# Physical staging sealing implementation scope

This implements the physical byte-finalization steps from section 7 of the
[preserved Pro review](2026-10-21-source-mutation-client-pro.md), not its complete
authority, provider or deployment contract. It is private and has no production
caller. Independent-profile startup remains disabled.

`pkg/fencing/stage_payload.go` receives bytes only for a ticket that the future
authority inventory has already durably reserved, including its producer/owner,
operation slot and byte budget. An existing directory and exclusive spool owner
are preconditions, not facts established by this helper. No PVC, scheduling,
filesystem-lock or runtime-fencing guarantee is inferred from opening a directory.

The implementation bounds each temporary file to the declared length, up to the
existing 64 MiB checkpoint block size. It rejects short, long or hash-mismatched
input before finalization. It syncs the file, closes the writable descriptor,
links a final ticket-specific name without replacement, and syncs the directory
before returning a descriptor. A concurrent finalizer must verify the winner's
actual bytes; read-only file permissions alone are not a fencing mechanism.

An uncertain directory sync can leave a final file with no returned descriptor.
Lookup-only recovery verifies the exact length/hash and re-establishes both sync
barriers. It does not receive replacement bytes. A previously sealed ticket must
use that recovery path, never the still-receiving path after file loss. The
authority inventory must enforce that distinction before production wiring.

Tests cover actual bytes followed by source descriptor retention and original
sequence allocation, a lost response/reopen, invalid lengths/hash/size, injected
file/directory sync failure, conflicting concurrent finalizers, symlink/path
rejection, missing directory/file, empty input and a full 64 MiB block. Their
existence is not a claim that a particular CI run passed.

## Explicitly unfinished

- Durable authority ticket inventory, global/per-producer quotas and concurrent
  receiving/finalizing rights, before any payload stream is accepted.
- Deployment-wide spool identity, ownership transfer and supported failure
  domains, pending the separately submitted Pro consultation.
- Sealed-descriptor persistence and producer-CAS attachment/cancellation;
  physical finalization alone neither admits work nor acknowledges a mutation.
- Crash-discovered temporary/final orphan accounting. Normal returns remove only
  their own temporary name, but process crashes can leave aliases. No sweeper or
  payload deletion API is installed; accounting must not be released prematurely.
- Durable deletion eligibility/tombstones, writer closure and compaction
  coordination; sealed bytes are not automatically reclaimed.
- Authenticated MutationClient, provider-only execution/credentials, actual
  bootstrap registration ACK, archive HEAD wiring, runtime agent and real
  independent-profile GCS/Chaos qualification.

These tests do not replace the first complete vertical path required by the Pro
review. In particular no authority restart/reschedule or cloud durability is
qualified by reopening a temporary directory in a unit test.

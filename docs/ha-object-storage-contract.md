# HA persistence and object-storage contract

## Intended architecture

ObjectStorage is the durable source of truth for admitted source mutations,
their payloads, execution outcomes, and recovery evidence. QuePaxa remains the
live consensus system: it decides membership and quorum-backed control state;
ObjectStorage does not replace consensus. The operator observes those systems
and coordinates actions. It must not invent consensus, receipts, terminal
outcomes, or proof that an old execution can no longer act.

NoPVC is a persistence constraint, not a recovery mode: durable truth must not
depend on a Pod PVC, node-local directory, or a particular node surviving. A
local retry cache may hold a caller's not-yet-admitted request; losing it can
require that caller to resubmit, and does not make it canonical state. Once a
mutation is admitted, its obligation and evidence must be reconstructible from
ObjectStorage. Loss/restart qualification must verify this boundary rather than
assume that every local retry record is durable truth.

Cooperative termination and crash recovery are internal evidence paths, not
user-selectable operating modes. They may establish different facts: a
cooperative drain can report completion, while a crash path must independently
prove the requested execution scope is excluded and settle every admitted
storage effect. Both paths must preserve the same durable truth and fail closed
when their required evidence is unavailable. This contract does not promise
that arbitrary crashes are immediately recoverable or that control-plane
actions physically fence a process.

## Required ObjectStorage capabilities

A provider is suitable for canonical HA state only when its documented and
verified semantics support all of the following:

- Atomic conditional creation and compare-and-swap for canonical mutable state,
  with conflict results that cannot be mistaken for success.
- Stable operation identity and exact preconditions across retries. A retry
  must not silently refresh a version condition or become a new operation.
- Exact-instance reads and deletion (or an equivalent durable tombstone) so
  compaction cannot remove data still needed by an admitted or delayed copy.
- A provider-specific way to account for every possible late effect, including
  SDK retries, multipart/resumable sessions, and lost responses. Such work must
  be durably described and recoverable, or disabled; hidden replay is not
  compatible with the contract.
- Durable verification of the exact operation and result before publishing a
  terminal receipt, advancing an archive watermark, or releasing capacity.

An ETag is an opaque validator, not proof of immutability, globally unique
identity, monotonic order, or exclusion of a delayed write. Content equality
alone likewise cannot prove which attempt produced an object. A provider may
use ETags or another opaque token only when its own documented conditional and
late-effect semantics satisfy the complete contract; code must not treat an
ETag as a numeric generation or infer that it is an immutable version.

## Current implementation and qualification boundary

The implementation is not yet provider-neutral. The current mutation profile
is GCS-specific (`gcs-json-v1` / `gcs-resumable-v1`) and represents object
versions with numeric GCS generations. The fenced bucket adapter rejects
versions it cannot interpret as those generations. Canonical authority-state
and evidence stores can use generation or opaque ETag CAS tokens and bind a
nonce-backed logical revision; that algorithmic support does not qualify any
specific provider implementation. These are implementation constraints, not
the definition of ObjectStorage and not a claim that other providers are unsafe
in principle.

The current source-mutation proof profile for non-GCS providers, including S3,
is not implemented. Adding one requires a distinct, explicit provider profile,
end-to-end conditional operation and retry/session accounting, exact-instance
cleanup semantics, and failure/restart tests. Reusing the GCS profile with an
ETag or mapping an opaque token to an integer is not sufficient. GCS
qualification is one product profile; it is not generic object-storage
qualification.

The existing design and evidence boundaries are described in the
[independent fencing plan](independent-fencing-v2-plan.md),
[cooperative Kubernetes fencing notes](kubernetes-fencing.md),
[terminal-evidence retention contract](terminal-evidence-retention.md), and
the preserved [mutation contract review](reviews/2026-10-21-mutation-contract-pro-followup.md)
and [dead-pod recovery review](reviews/2026-10-21-dead-pod-autohealing-pro.md).
Those documents record implementation and qualification gaps; this contract
does not supersede them or claim production qualification that has not been
demonstrated.

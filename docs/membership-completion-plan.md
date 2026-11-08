# Membership completion: historical plan, not a current runbook

This was a release-era plan for manual and alpha1 automatic Operator workflows.
It is not a declaration that current generation DR is complete.
The [v0.23.1 original](https://github.com/mrchypark/rhiza/blob/2262531f266f2638eb21baea6f65de45465ffe90/docs/membership-completion-plan.md)
preserves the original plan, CI history and operational examples at an immutable
commit. Do not run its retired StatefulSet/fencer examples as an alpha2 guide.

## Current contract and ownership

Use the [current operator guide](../deploy/operator/README.md) for genesis,
Host healthy-quorum repair and certified engine responsibilities. Current normal
managed HA uses the explicit request/Graph append-only profiles, fresh voter
identities and certified ADD-first repair. A lost WAL never inherits an old
voter identity. Same-identity rejoin requires its original WAL and certified
membership. These statements do not retire optional raw externally fenced
membership APIs or their guards.

## Automatic recovery status

The former deployment anchor `automatic-recovery-with-kubernetes-fencing`
described the retired path. Its current replacement is the
[approved policy/status section](../deploy/operator/README.md#approved-automatic-attempt-policy-not-implemented-or-qualified),
not an installation recipe. Automatic generation DR remains unimplemented and
unqualified. Queue/status bookkeeping is not recovery execution.

Whole-generation activation still needs independent old-runtime termination,
recreation prevention, outstanding-storage-work evidence, complete successful
ACK source coverage and fresh live authority, followed by a new ready quorum.
Pod deletion is not fencing proof. Unknown evidence remains Blocked; pending
operations and UNKNOWN originals must not be cleared or silently migrated.

For the preserved safety boundary, see [recovery](recovery.md#managed-no-pvc-ha-and-manual-disaster-recovery),
[fencing status](kubernetes-fencing.md) and [anchor protocol](recovery-anchor.md).

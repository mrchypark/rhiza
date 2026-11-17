# Managed Membership Manual Recovery Boundary

Managed ADD/replacement preparation is fail-closed when its original
identity or membership state is not unambiguous.

## Automatic continuation

The existing replacement loop may continue without manual intervention only
when all of the following still describe the same operation:

- the status repair record contains the original full plan;
- the exact config and abort revision still match;
- the exact membership journal reservation still identifies that operation;
- the candidate Pod, learner registration, and prepared handoff have not
  diverged from that plan.

An unchanged `Preparing` repair is returned to the existing replacement path.
`Preparing` never closes SQL reservations by itself.

## Manual recovery boundary

Automatic recovery stops when any of these is true:

- the current membership or abort revision differs from the repair plan;
- membership reconfiguration is pending;
- the journal belongs to another operation;
- the original Pod, learner registration, handoff, or terminal evidence cannot
  be proven to match;
- repairing the state would require guessing ownership or abandoning an
  in-flight membership effect.

In these cases, the host reports `manual-recovery-required` and preserves the
status repair record and membership journal.

## Required operator actions

1. Preserve the full `RhizaCluster` status, especially `status.repair`, and the
   object-store membership journal. Do not delete or rewrite either object.
2. Preserve the exact Pod UID, learner registration, handoff files, and
   membership evidence associated with the recorded operation.
3. Determine whether the original operation identity can be restored exactly.
   If it can, resume the normal replacement loop; the existing exact-plan path
   will revalidate the plan before proceeding.
4. If exact identity cannot be restored, stop automatic membership
   reconciliation for this cluster and escalate to the repository owner. The
   current public recovery API has no verified generic command that safely
   abandons this state.

Do not directly open SQL reservations, delete the journal, retarget the repair,
or force `Closed`/`Completed` status as a recovery shortcut. SQL reservations
remain open while the repair is `Preparing`; preserving them is
safer than manufacturing an unsupported abort proof.

Automatic reservation abort and ownership transfer remain disabled. The common
object-store API does not atomically return object bytes together with the
version used for a conditional write. Provider-specific conditional writes are
therefore not treated as sufficient proof for reopening or deleting managed
membership state.

## Evidence to retain

- the exact status repair record and resource version;
- the membership journal bytes and object version, when available;
- the candidate Pod UID and owner chain;
- learner registration and handoff evidence;
- the latest membership history and exact terminal, if one exists;
- host/operator logs showing the last completed and attempted stage.

The repository does not currently provide a tested command sequence for
clearing an unresolved repair. Do not infer one from low-level APIs or delete
objects to bypass this boundary.

# Kubernetes fencing: retired operational guide

The alpha1 `RhizaFence`, StatefulSet, `rhiza-fencer serve` and
`rhiza-fencer supervise` installation described here is retired, not the
normal managed HA installation. Do not apply those manifests or enable legacy
automatic-recovery flags as an alpha2 procedure. There is no implicit migration
or relabeling of legacy authority, pending operations or storage.

The [v0.23.1 original](https://github.com/mrchypark/rhiza/blob/2262531f266f2638eb21baea6f65de45465ffe90/docs/kubernetes-fencing.md)
is preserved at its immutable release commit for historical protocol and test
provenance. Its deployment commands and qualification claims are not current
operating instructions.

## Current managed HA

See the [current operator guide](../deploy/operator/README.md) for genesis,
healthy-quorum Host repair and certified engine membership responsibilities.
Normal HA does not require a separate always-on fencer or external lease.
Healthy repair and whole-generation disaster recovery are different operations.

Automatic generation DR is not implemented or qualified. Existing
`RhizaRecovery` queue/status components do not establish an executable recovery
controller. The [approved automatic-attempt policy](../deploy/operator/README.md#approved-automatic-attempt-policy-not-implemented-or-qualified)
defaults OFF; it is not a claim that an enabled runtime can execute it.

## Safety boundary retained for independent recovery

Any separately authorized recovery must establish:

- Complete old runtime identity and incarnation scope, including voters,
  learners, sidecars, embedded processes and storage-maintenance writers.
- Actual independent termination and prevention of recreation, including old
  container restarts, rescheduling and restored machines. Pod DELETE success,
  API absence, timeout or an unreachable process is not this proof.
- Resolution or safe containment of outstanding storage work, including already
  submitted writes and deletes. A quiet object listing is not a clean drain.
- A verified source covering successful ACKs, with authentic checkpoint and
  contiguous certified suffix evidence. Unknown source coverage remains unknown.
- Fresh live activation authority bound to the exact operation, source,
  storage, binding and generation. Historical receipts or an expired authority
  observation do not authorize new activation or future writes.
- A new ready quorum before service resumes. Unknown evidence or a partition
  stays Blocked for manual action, not a fabricated proof or bootstrap.

Preserve unresolved registrations, pending application state, UNKNOWN outcomes
and anchor/Transition evidence. Never delete a ledger, clear pending state or
reset object storage to manufacture recovery. Optional manual DR evidence and
raw externally fenced membership interfaces remain separate; retiring this
launcher does not remove their safety guards.

See [recovery boundaries](recovery.md#managed-no-pvc-ha-and-manual-disaster-recovery)
and [anchor protocol limits](recovery-anchor.md).

# Recovery Anchor: protocol boundary, not automatic DR

The anchor provides CAS and verification through `recoveryanchor.Coordinator`.
It does not terminate old runtimes, restore data or orchestrate a new generation.
The current operator does not implement automatic generation DR; a
`RhizaRecovery` queue/status record is not permission or a completed executor.

Normal managed HA does not require this as an always-on external lease/fencer.
For genesis, Host healthy-quorum repair and engine membership roles, use the
[current operator guide](../deploy/operator/README.md). Its
[automatic-attempt policy](../deploy/operator/README.md#approved-automatic-attempt-policy-not-implemented-or-qualified)
is explicitly unimplemented and unqualified.

The [v0.23.1 original](https://github.com/mrchypark/rhiza/blob/2262531f266f2638eb21baea6f65de45465ffe90/docs/recovery-anchor.md)
preserves the former alpha1/StatefulSet service configuration and
application-specific migration notes at an immutable commit. Those examples
are not current alpha2 deployment or migration instructions.

## Activate protocol

Two-phase CAS through `Coordinator.Activate` reserves `Record.Transition`
with the frozen source binding, generation and evidence, then calls the
Verifier for a matching target binding and proof. Commit advances the generation,
clears Transition and records a receipt. Recovery reserves Transition, not an
application PendingWrite; a pending application operation must not be cleared
to manufacture recovery.

`RequestHash` covers the entire request, including `FenceHash`. The latter
describes independent fencing evidence and is not the request hash.
Independent proof must establish the complete old-runtime termination scope,
recreation prevention and outstanding storage-work safety. A Pod deletion,
timeout or successful infrastructure API response alone is insufficient.
The verified recovery source must cover successful ACKs; activation also needs
fresh live authority and a new ready quorum. Unknown evidence stays Blocked.

## Verification is not restoration

`Coordinator.Verify(ctx, req, receipt)` checks the live record, committed
request and matching receipt/binding/generation. A stored receipt alone cannot
authorize activation, future writes or a cached authority decision.
Verify does not restore checkpoints or establish full database rollback
protection. Same-generation startup must compare application state with the
current external evidence.

`CheckBinding` rejects pending writes and active Transition; it is non-atomic
and must be used within the application's write protocol.
`UpdateApplication` CAS-updates application evidence/pending state while
preserving recovery fields, with no active Transition.

## Evidence and preservation

Application evidence has an application-specific schema. A static epoch/token
proof is not a proof of the complete database or every ACK.
Object-store `GenerationAnchor` and the external Record have distinct roles:
the former is an immutable recovery artifact; the latter supplies live
binding/evidence/transition authority. A missing external anchor is an error,
never a bootstrap signal reconstructed from restored data.

Preserve legacy UNKNOWN/pending originals and all binding/generation/Transition
evidence. There is no implicit compatibility or automatic migration from a
legacy ConfigMap schema. Validate any separately authorized migration against
the original format and trusted storage identity; do not reset epochs,
bootstrap missing authority or discard unrecognized fields.
An authenticated, scoped verifier is required; missing credentials must fail
closed, not disable authentication. Historical TLS/auth examples do not
establish that a current deployment or end-to-end DR path has been qualified.

# Recovery and voter identity

Rhiza recovers certified data inside the database. The host application,
systemd, or Kubernetes starts a stopped process with its original disk.
Neither a process restart nor an object-store checkpoint grants a replacement
process permission to forget an existing voter's recorded proposals.

## Supported recovery boundary

| Event | Recovery action |
| --- | --- |
| Restart with the original intact WAL | Restore recorder state and replay certified decisions. |
| Detected SQLite/Graph open failure | Quarantine materializers and rebuild from retained certified history. |
| Voter falls behind | Fetch certified peer/archive history; restore a pinned checkpoint when needed. |
| One voter unavailable | The remaining voters still hold a strict majority, so quorum operations continue. |
| Unavailable voters leave fewer than a strict majority (two of three, three of five) | Restore the original nodes/disks/network. Quorum operations remain unavailable; local reads on running nodes remain possible. |
| Voter WAL permanently lost | Never reuse its registered identity with an empty WAL. With a surviving quorum, use certified ADD-first replacement with a fresh voter ID and WAL; otherwise restore the original quorum or stop for manual DR. |
| Membership change or learner promotion | Use authenticated QuePaxa reconfiguration with a surviving quorum and durable learner admission. Editing the bootstrap member list is not reconfiguration. |

`Ready()` describes completed local startup/catch-up, not current quorum
availability. Do not turn quorum loss into a liveness policy that restarts all
voters or replaces their volumes. Recovery-time Record RPCs remain necessary
for voters with intact state to resolve unfinished slots during a full restart.

Normal managed HA adds a fresh learner, verifies its certified prefix and WAL
identity, commits its admission, then removes the failed identity through the
surviving quorum. In the reference deployment this sequence is coordinated by
the managed host's [background replacement loop](../cmd/rhiza-ha-init/serve.go)
and [Host coordinator](../pkg/managedsql/replacement.go), not by a general
whole-generation recovery loop in `rhiza-operator`. Kubernetes maintains the
Pod replicas; the engine establishes voting authority. A lost or removed voter
ID is never reused. Same-identity
rejoin requires its intact original WAL and the current certified membership;
an empty WAL cannot acquire voting authority by replaying an archive.
This path does not require a separate fencer or external lease service.
It does not prove that an isolated process ended or that external work drained.
Passive fixed read replicas remain outside membership reconfiguration.

## Voter registration and upgrades

Multi-voter startup binds a local WAL identity to an immutable registration in
the existing shared object store. Membership identities and voting credentials
are bound to the registration; endpoint addresses may change when moving the
original disk. Registrations are not leases and cannot be stolen or expired.
They are not deleted by Rhiza GC.

Missing or inconsistent voting state returns `ErrVoterStateLost` before a peer
listener can accept votes. A storage access failure also prevents startup;
it is not permission to create another identity. An original pre-registration
WAL returns `ErrVoterEnrollmentRequired` and needs a one-time offline upgrade:

1. Stop **all** voters, including any older binaries, and disable automatic
   restarts while enrolling. Confirm that each original WAL is intact and that
   no duplicate instance can reconnect. Preserve the disks and object store.
2. For each voter, keep its existing environment/configuration and run
   `rhiza --enroll-existing-voter`. This takes the WAL lock, registers its
   identity, closes resources, and exits without starting peer or HTTP listeners.
   Embedded Go hosts may call `rhiza.EnrollExistingVoter(ctx, config)` instead.
3. After every original voter is enrolled, restart all voters using the updated
   binary and the same membership, credentials, namespace and original disks.

Enrollment is an operator assertion about the original state, **not** repair
for a lost disk. It cannot replace an existing registration, restore forgotten
votes, or turn an empty replacement into an old voter. Do not remove identity
files, registrations, WALs, or object-store objects to work around an error.
A first initialization interrupted before its local identity is persisted can
also require offline enrollment; it must never be guessed to be a new voter
from an empty WAL alone. Fresh clusters must use an unused cluster namespace. Mixed old/new binaries
are not a safe migration path because old binaries do not enforce registration.

The guard does not establish exclusive execution for cloned complete disks or
detect an intentional rollback of both disk and shared storage. Original-disk
exclusivity, prevention of old-instance reconnection (fencing), and durable,
non-rollback storage remain deployment requirements. Single-voter archive
restoration retains its existing behavior and requires the old instance to be
stopped permanently; it is not a multi-voter replacement protocol.

## Checkpoints and remote durability

A checkpoint's certified seal must be durably published to the archive before
`CURRENT` advertises that checkpoint. HA object data is append-only; online
delete GC is not part of the HA contract. Certified checkpoint and archive
validation still protect recovery. Local-mode restore pins and GC have their
own contract and are not removed by this boundary.

Graceful close drains and joins local work and syncs/closes the WAL. For
archive-publishing voting nodes, it also publishes the final local certified
prefix; reconfiguration non-voters retain their publication restriction. Close
does not force a new
checkpoint or require quorum to certify one during shutdown. Recovery still
verifies the retained certified checkpoint base (if any) and replays its
archived suffix; an untrimmed certified archive can be replayed without a
checkpoint. Archive publication or local close failures remain close errors.

`before-ack` adds remote publication after quorum certification. Given intact,
non-rollback remote storage and a valid consensus history, it preserves the
certified prefix containing successful acknowledgments. It neither replaces
quorum nor recovers a lost voter's outstanding recorder state. In runtime
paths that support `async`, permanent loss of the authoritative voter disks
can lose acknowledged changes newer than the last published prefix. The
publication interval is not a guaranteed maximum loss window. The managed
initializer and Host do not accept `async`.

## Managed No-PVC HA and manual disaster recovery

The namespace-scoped [managed operator](../deploy/operator/README.md) uses
v1alpha2 Deployment Pods and init-container identity handoff. The public managed
Host requires before-ack durability. Healthy-quorum replacement uses the
certified ADD-first path above, not a separate fencing Deployment.
The operator validates the installed profile and reconciles initialization,
genesis and bootstrap plans. The reference host runs the repair loop; upgrading
the library alone does not add that loop to an arbitrary embedding application.
The two init containers prepare identity and wait for the committed plan; the
main engine performs verified startup/adoption and catch-up. NoPVC uses an
`emptyDir`, so a container restart and a new Pod are different recovery events.
`voterCount` is fixed at creation (3–64), not an autoscaling API. Recreate rollout
enforcement and peer/client Service management are not supplied by this
initializer. See the [Pod builder](../pkg/operator/bootstrap_members_v1alpha2.go)
and [initializer](../pkg/operator/initializer_v1alpha2.go) for those boundaries.

If quorum is lost, writes and linearizable reads fail; ready nodes can still
serve local reads. First restore the original nodes, disks or network. Missing
Pods, failed probes and object-store checkpoints cannot manufacture quorum or
authorize a generation change.

Whole-generation DR is separately authorized, not an automatic fallback
implemented by the current operator. The approved design defaults automatic
attempts to OFF. Explicit policy may trigger preparation after sustained
actual ReadIndex/quorum failures, but failure detection is not activation
authority. This automatic-attempt policy is not yet implemented or qualified.
An `automaticRecovery: OFF` schema default or an accepted recovery request/status
API is not a DR executor. Do not interpret another accepted policy value as
execution support or a safe way to activate a new generation.

The DR design requires independent evidence of termination for the complete
old writer, voter, learner and sidecar scope, prevention of recreation, and
safe resolution of in-flight storage work. A verified source, live activation
authority and a verified new ready quorum are required before service resumes.
For `before-ack`, the certified source must cover every successful ACK.
Unknown evidence or partition leaves recovery Blocked for manual action.
No new lease, epoch or live permissions are introduced. Pending writes,
UNKNOWN outcomes and live anchor/Transition checks remain protected.
A Pod delete response, elapsed timeout or stored receipt
alone is insufficient. Preserve all evidence and pending application state;
missing evidence blocks DR. Legacy alpha1 `spec.membership`, `spec.fence` and
StatefulSet recovery examples are unsupported by the current alpha2 API and
must not be applied as an operational procedure.

The source generation's immutable membership record binds its durability mode.
The historical recovery design requires explicit data-loss authorization for an
`async` source even when the target uses `before-ack`. The separately approved
managed Async proposal permits only unpublished-tail loss beyond the verified
published cut. Neither is an implemented managed DR procedure or an alpha2
`allowDataLoss` field. The current [published-archive rehearsal](managed-async-recovery.md)
prepares isolated local data without activating a writer.
Changing an environment variable cannot retroactively protect past
acknowledgements. Unknown legacy durability records fail closed and must not
be rewritten or removed to bypass validation. Archive age is evidence age,
not a bound on the number or age of missing acknowledged writes.

`before-ack` protects successful write acknowledgements under the remote-store
and fencing contract. It does not promise preservation of every earlier read:
a concurrent read or request-status lookup can observe an applied write before
that write's remote publication and acknowledgement complete.

Empty replacement voters in an already-started generation still fail with
`ErrVoterStateLost`. Replaying the recovery manifest does not give such a voter
permission to forget newer votes. Retain the original source archive and all
registration/lineage records; do not clear them to force recovery.

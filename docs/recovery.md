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
| One of three voters unavailable | The remaining majority continues quorum operations. |
| Two of three voters temporarily unavailable | Restore the original nodes/disks/network. Quorum operations remain unavailable; local reads on running nodes remain possible. |
| Voter WAL permanently lost | Refuse automatic reuse of its registered identity. Recover the original voting state or use a separately designed, explicitly fenced disaster-recovery procedure. |
| Membership change or learner promotion | Not an online operation currently supported by Rhiza. Editing the member list is not reconfiguration. |

`Ready()` describes completed local startup/catch-up, not current quorum
availability. Do not turn quorum loss into a liveness policy that restarts all
voters or replaces their volumes. Recovery-time Record RPCs remain necessary
for voters with intact state to resolve unfinished slots during a full restart.

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
`CURRENT` advertises that checkpoint. Initial recovery pins its selected
shared CAS archive history and checkpoint files against concurrent trimming/GC until
replay and installation finish. A lost recovery lease fails recovery instead
of silently continuing with an unprotected source.

`before-ack` adds remote publication after quorum certification. Given intact,
non-rollback remote storage and a valid consensus history, it preserves the
certified prefix containing successful acknowledgments. It neither replaces
quorum nor recovers a lost voter's outstanding recorder state. `async` can lose
acknowledged changes newer than the last published prefix after permanent
loss of the authoritative voter disks; the publication interval is not a
guaranteed maximum loss window.

## Operator responsibilities

A recovery Operator can restore original processes, disks and connectivity
after a temporary majority outage. For permanent loss it must distinguish
missing voting state from ordinary lag, fence old instances and use a supported
re-registration or disaster-recovery protocol. Rhiza currently refuses the
unsafe replacement; it does not implement that protocol or automatic membership
changes. An Operator must not bypass this refusal by reducing quorum, clearing
registrations or promoting a learner through configuration alone.

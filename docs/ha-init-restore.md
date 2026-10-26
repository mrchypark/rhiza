# Go HA init restore and logical storage-write authority — implementation checkpoint

This document records the first implementation checkpoint, not a qualification
claim. Generic HA checkpoint staging and actual `Node.Open` staged-file adoption
are committed in `dc1a436`. The logical-writer startup/mutation path and required
`restore-init` CLI/deployment template are implemented in the current worktree,
but remain unqualified. Init completion, Pod deletion, Pod absence, and a new
Pod UID establish neither voter admission nor storage-write exclusion.

The no-CRI logical-writer code path does not remove the existing physical
runtime-agent/CRI implementation or its deployment assets. The replacement
profile avoids those components, but deleting the old path is deferred until
the new lifecycle is integrated and qualified. Pending-learner promotion,
quorum-loss DR, and fresh empty-genesis bootstrap remain incomplete gates.

## Replacement lifecycle

For ordinary StatefulSet recreation with a strict quorum alive, the replacement
Pod gets a fresh `emptyDir`. A required Go init reads the authorized archive and
stages the exact selected checkpoint files outside `RHIZA_DATA_DIR`. It must not
open the qlog, create voter identity, register a writer, or mutate the archive.
The main process reopens current recovery evidence, verifies the original staged
files against the exact checkpoint root and recovered prefix, installs them
through the existing materializer, replays the certified suffix, then follows
the existing learner, catch-up, membership, and Ready gates. It must not reuse
the old Pod's voter/WAL identity merely because the StatefulSet ordinal is the
same. A same-Pod container restart is different: init does not rerun and the
existing WAL/identity path remains authoritative.

Quorum-loss recovery is separate. It must first establish exclusion from the
old storage-write namespace, then produce and verify an authorized successor
generation. The successor uses its own canonical generation, membership, and
storage namespace. The generation anchor binds the restored history and target
membership; it does not itself revoke an old process's storage credentials.
Restoring checkpoint bytes cannot resume an old voter or create quorum.

No-PVC only means local disk state does not survive Pod replacement. Forced
delete, StatefulSet partitioning, an absent Pod, or init success is not proof
that an old process can no longer write. An independently enforced revocation
of its storage capability can suffice without proving physical death; absent
that proof, the transition remains blocked.

## Reuse and ownership boundary

Go Local recovery has its own typed authority: `LocalStoreUUID` and
`ConfigDigest`, bound to its exact WAL/checkpoint tuple. Local automatic
backup/full-loss restore and its `before-ack` default are not HA quorum
authority. HA recovery relies on voter registration, membership, consensus
history, and generation lineage. Neither mode's authority can substitute for
the other's.

The reusable checkpoint boundary is `pkg/checkpoint.Manager`: HA uses
`OpenRoot` and `DownloadAndVerifyRootFiles` to validate exact roots and their
content-addressed files. `materializer.Materializer.RestoreCheckpoint` is the
existing install path. Local's `Manager.CreateLocalFiles`, `LocalBinding`,
`StoreUUID`/`ConfigDigest`, `local-store.json`, `.local-backup-import` journal,
and Local snapshot origin must not be copied into HA. Local's Kubernetes
fixture only stages files and has main independently call `rhiza.Open`; it
does not prove HA main consumes init-staged files.

The restore core and main adoption are committed in `dc1a436`; the required
init entry point and deployment integration are implemented in the current
worktree:

- `pkg/recovery/ha_restore_verification.go` verifies a fixed archive snapshot,
  certified checkpoint, membership, and optional generation anchor.
- `pkg/node/ha_restore_staging.go` stages exact checkpoint files and an index,
  with atomic publication and existing-stage revalidation.
- `pkg/node/node.go` contains a `HAInitRestoreDir` adoption branch. On a clean
  start with no local voter identity/WAL, it checks the staged bootstrap
  membership, reopens the selected checkpoint root, verifies staged file bytes
  against root metadata and recovered history, passes those file paths to
  `RestoreCheckpoint`, then replays the archive suffix.

The `Node.Open` adoption oracle now exists in
`pkg/node/ha_restore_adoption_test.go`: it stages a real certified checkpoint,
removes its remote checkpoint blocks, verifies that `Node.Open` restores the
seeded value and applies a new tail, then verifies that reopening the same
DataDir does not adopt the stale stage or roll back the newer WAL. A second
case rejects equal-tip staged-file tampering. The required init CLI and
replacement template now exist, but their presence and these local tests are
not Kubernetes Pod replacement, live storage, or stale-writer qualification.
The stage index is integrity evidence, never authority. An intact existing
WAL/identity makes the stage ineligible, preserving same-Pod continuity.

## Pro-reviewed no-CRI authority direction and implementation status

The preserved Pro response at
`/Users/cypark/.codex/task-state/rhiza-review-followups/pro-no-cri-authority-response-20261026.txt`
reviewed baseline `42857613dc6c6c505c692d245d203fe0b54b8e16`, not this
worktree's implementation. Its design conclusion is to reuse the existing
canonical authority and source-mutation journal with an explicit logical writer
binding, without CRI-agent dependence in that path. It did not review or approve
the implementation below. The current source adds that profile/path, but this
status is based on local code/tests and is not a production qualification.

The proposal is not “query a boolean permission, then let the Pod write GCS.”
The authority must accept, execute, and account for the exact operation. A
correct process may pause after an allow response and resume after revocation;
that does not require Byzantine behavior. Every remote mutation—including
archive/checkpoint writes, recovery pins, registration objects, deletes, and
GC—must pass through the authority-held writer path. Pods and init need
read-only object-store credentials; there is no direct-write fallback.

### Minimal binding and record contract

Use a discriminated, versioned root profile and a logical binding, rather than
weakening `RuntimeBinding` or populating it with fabricated values. The
following is the implemented versioned binding shape:

```text
LogicalWriterBindingV1 {
  version: 1
  profile: "logical-writer-v1"
  sourceClusterID: exact source generation
  nodeID: authority-validated QuePaxa identity
  runID: unique process/WAL run
  walIdentity: exact main-process qlog identity, bound once
  producerUID: authority-issued deterministic producer identity
  storeScopeHash: provider + bucket + prefix + source scope
  role: authority-derived active-voter or pending-learner role
}
```

The logical root must declare its schema version/profile and exact source scope.
The existing root owner identity may remain a stable controller/workload
namespace key; it is not per-container evidence and must not be treated as
proof of liveness. `NodeID`, `RunID`, and `WALIdentity` identify the logical
incarnation. `PodUID` may remain registration/provenance metadata, but its
absence or change never proves a writer stopped. Role, membership, scope, and
producer are selected/validated by authenticated authority state, not accepted
from annotations or caller-provided identity fields.

Keep the writer-binding profile distinct from the existing execution profile
(`gcs-json-v1` / `gcs-json-held-v2`). The root profile chooses the record
codec and validation rules; the execution profile chooses the provider
operation contract. A pending learner has no source-mutation producer. The
authority assigns producer UID and scope and binds the actual WAL before
writer activation.

Implemented code ownership/API shape:

- `pkg/fencing/logical_binding.go`, `ledger.go`, `mutation_journal.go`, and
  `authority_client.go` implement the explicit logical profile, logical Run and
  producer binding, authenticated registration/WAL bind, and journal-backed
  mutation client. Physical artifacts and legacy digest shapes remain separate.
- `pkg/operator/logical_authority.go`, the existing authority handler, mutation
  executor, receipts, GCS provider, and source-mutation transports implement
  canonical registration, scope/stage binding, admission, dispatch, settlement,
  and logical closure handling. The profile is GCS-only.
- Node startup in the logical profile connects directly to the existing
  authenticated authority and routes protected object mutations through the
  journal/authority executor. The authority assigns and validates logical
  identity/scope; the grant is not a storage credential.
- `pkg/node/ha_restore_prepare.go` and the `restore-init` command verify and
  stage an existing certified HA checkpoint using a read-only object-store
  wrapper. The main process revalidates and consumes staged files through the
  existing materializer/recovery path. The replacement manifest and public
  membership projection are present.
- Existing storage-closure consumers remain distinct from process-terminal
  consumers. A logical storage closure is not `Drain`, `Exited`, a physical
  terminal receipt, or a `BarrierComplete` claim. Do not generate or re-label
  those legacy artifacts. Existing CRI/runtime-agent code and deployment assets
  have not been removed.

Close must CAS the producer admission state and freeze `ClosedThrough` at
`LastAdmitted`. This only blocks future admission/dispatch; it does **not** prove
that old admitted effects cannot complete later. Preserve all previously
granted dispatch rights as obligations and settle them or prove their future
effects excluded against exact object lineage before activating another writer.
The existing `settleEngineRepairProducer` storage-closure path is a reuse
candidate for ordinary voter replacement, not evidence that runtime-dependent
validators already accept a logical root.

### Lifecycle and evidence boundaries

Ordinary replacement remains same-generation: while quorum is alive, close the
exact removed logical producer cohort, settle its storage effects, commit REMOVE,
then learner catch-up/ADD and activate the new writer. It does not need a fresh
prefix every time. DR is different: close and inventory the whole source
generation; prove the certified source and all referenced storage effects;
bind closure proof, source frontier, and target lineage together; only then
activate a distinct target namespace. Prefix isolation helps only if old
credentials cannot write target objects and GC cannot cross generation refs.

For NoPVC automatic DR that promises preservation of successful client ACKs,
before-ack durability is required. With an old quorum still alive, denying its
GCS writes is insufficient if its local path can still return a successful
ACK. Storage-write exclusion does not claim stale-read or old-service
exclusion. Unknown provider outcomes stay pending; Pod deletion, API absence,
timeouts, quiet buckets, a new Pod UID, or a Boolean lease do not settle them.

The prototype provider is GCS-only. No logical-binding implementation or
provider-neutral contract exists yet for S3/Azure. The design remains blocked
for any profile whose late operations, conditionals, retries, deletions, pins,
or GC cannot be accounted for. Read-only init remains separate: it performs
no remote pin/claim/write; if source evidence disappears during staging, it
fails. Normal main startup may use its existing recovery pin lifecycle.

### Compatibility and migration

No automatic flag-based conversion is allowed:

| Existing state | Proposed handling |
|---|---|
| New root | Create with explicit logical schema/profile and required journal. |
| Runtime-bound strict root | Preserve runtime meaning; migrate only through an explicit operation that closes old producers and verifies effects before a new logical root/profile. |
| Cooperative/direct-write root | Reject automatic upgrade; unjournaled old writes cannot be retroactively inventoried. |
| Unknown, mixed, malformed, or unsupported profile | Reject startup and writer grant; do not set `RequireRuntimeBinding=false` or `RequireMutationJournal=false` as a downgrade. |

Same-Pod main restart preserves and validates its existing WAL. A fresh Run
must close/settle the preceding Run before receiving a new producer. A new
Pod's empty storage starts with a fresh logical identity and normal learner
admission; it cannot resurrect a retired grant.

### Evidence status and acceptance gates

**Pro review:** design GO for the no-CRI direction; NO-GO to treating existing
flags as an implemented mode. The review is baseline-scoped and did not run
tests or approve this uncommitted code.

**Current source status:** the explicit `logical-writer-v1` profile, direct
authority registration/WAL binding, logical mutation journal path, and GCS
storage executor are implemented in this worktree. Legacy runtime-bound roots
remain distinct; profile selection does not convert them. Ordinary active-voter
startup and source mutation are implemented, but pending-learner promotion and
assigned replacement are not yet a complete end-to-end lifecycle. The operator
closure path supports bound-WAL closure and typed unbound zero-work closure;
missing or unreadable producer state fails closed. Do not infer writer exclusion
from stop, Pod absence, or process status.

**Current restore status:** generic verifier/stager and `Node.Open` adoption are
committed in `dc1a436`; required `restore-init` CLI and the logical replacement
template/public membership projection are implemented in this worktree. Local
tests do not establish Kubernetes Pod replacement, live GCS, DR, or stale-writer
qualification. Fresh bootstrap has no authorized empty-genesis selection and
must fail closed when no certified source exists.

**Provider and qualification boundary:** the new logical mutation executor is
GCS-only. Its local Floci/emulator workflow is opt-in test-only and does not
qualify a production GCS bucket. No equivalent logical profile contract has
been established for S3 or Azure. Exact-SHA SDK/adaptor contracts across all
providers, live GCS, Kubernetes replacement, quorum-loss DR, and old-writer
denial remain separate uncompleted acceptance gates.

Remaining implementation and qualification gates:

1. Complete assigned learner registration, WAL recovery, REMOVE/ADD evidence,
   promotion, and activation using the logical binding, without physical-runtime
   fakes or weakening legacy terminal proof.
2. Implement separate full-inventory, source/target-bound closure consumption
   for quorum-loss DR. Require before-ack for DR that promises to preserve
   successful client acknowledgments.
3. Integrate fresh-cluster bootstrap only when the operator can authoritatively
   select and persist a certified genesis/archive source; missing archive is not
   an empty-cluster signal.
4. Run exact-SHA object-store contracts through real SDKs/adapters for every
   claimed provider. Keep emulator tests, live GCS, Kubernetes replacement, and
   DR old-writer denial as separate evidence levels. Remove the old CRI profile
   only after the replacement lifecycle is implemented and qualified.

Required adversarial process tests include allow-then-pause/revoke/resume,
root-read versus producer-close races, dispatch persisted before provider call,
commit with response loss, duplicate authority workers, held G0 mutation versus
GC/recreation, pending delete of a new-root referenced generation, old-grant
access to a successor prefix, old quorum ACK attempts after DR, and missing
activation/terminal ACK. All must fail closed without fabricating drain,
terminal, or process-exit evidence.

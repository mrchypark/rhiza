# `pkg/operator` current contracts appendix

Scope and source root are as stated in
[`operator-go-reverse-spec-2026-10-30.md`](operator-go-reverse-spec-2026-10-30.md).
All source references resolve to the canonical project checkout.

## Go/JSON resource shapes

```go
type Resource struct { APIVersion string `json:"apiVersion"`; Kind string `json:"kind"`; Metadata map[string]any `json:"metadata"`; Spec Spec `json:"spec"`; Status Status `json:"status,omitempty"` }
type Spec struct {
  StatefulSet string `json:"statefulSet"`; Container string `json:"container,omitempty"`
  SourceClusterID string `json:"sourceClusterID"`; Durability string `json:"durability"`
  RecoveryID string `json:"recoveryID,omitempty"`; AllowDataLoss bool `json:"allowDataLoss,omitempty"`
  MaxArchiveAgeSeconds int64 `json:"maxArchiveAgeSeconds,omitempty"`; Fence Fence `json:"fence,omitempty"`
  AnchorID string `json:"anchorID,omitempty"`; Membership *MembershipSpec `json:"membership,omitempty"`
}
type Fence struct { RecoveryID string `json:"recoveryID"`; ClusterID string `json:"clusterID"`; StatefulSetUID string `json:"statefulSetUID"`; Confirmed bool `json:"confirmed"`; Evidence string `json:"evidence"` }
```

`MembershipSpec` has required `operationID`, `remove`, `fence`, `voterPods`,
`replacementPod`, and `replacementSecret`; optional `removedPod` and
`abortAddition` use `omitempty`. A `VoterPod` is `{nodeID, pod}`. Membership
fence is `{nodeID, walIdentity, workloadUID, confirmed, evidence}`. The
membership path takes precedence over normal recovery in `reconcile`.

`Status` JSON fields are `phase`, `message`, `observedGeneration`, `source`,
optional `target`, `recoveryID`, `specHash`, `statefulSetUID`,
`sourceDurability`, `sourceMembership`, `manifestHash`, `recoveredTip`,
`secretName`, `stage`, `archiveCapture`, `peers`, `targetIdentities`,
`membership`, `anchorRequest`, and `anchorReceipt`. Membership status has
`phase`, optional `removeRequest`, and optional `add`. Add journal has
`operationID`, `expectedConfigID`, `expectedAbortSlot`, `memberID`,
`walIdentity`, `requestHash`, `secretUID`, and `secretResourceVersion`.

```go
type ClusterSpec struct {
 LogicalID string `json:"logicalID"`; StatefulSet string `json:"statefulSet"`
 Container string `json:"container"`; AdoptClusterID string `json:"adoptClusterID"`
 VoterCount int `json:"voterCount"`; HTTPPort int `json:"httpPort,omitempty"`
 PeerPort int `json:"peerPort,omitempty"`; ClusterDomain string `json:"clusterDomain,omitempty"`
 Bootstrap bool `json:"bootstrap,omitempty"`; AnchorID string `json:"anchorID,omitempty"`
 Automatic bool `json:"automatic"`; Policy AutoPolicy `json:"policy"`
}
type ClusterStatus struct { Phase string `json:"phase"`; Message string `json:"message"`; ActiveClusterID string `json:"activeClusterID,omitempty"`; ActiveRecovery string `json:"activeRecovery,omitempty"`; ObservedGeneration int64 `json:"observedGeneration"` }
```

The complete operator-owned automatic persistence schemas are:

```go
type autoControl struct { Version int `json:"version"`; BindingUID string `json:"bindingUID"`; BindingHash string `json:"bindingHash"`; BindingVersion int `json:"bindingVersion,omitempty"`; LegacyBindingHash string `json:"legacyBindingHash,omitempty"`; StatefulSetUID string `json:"statefulSetUID"`; ActiveClusterID string `json:"activeClusterID"`; Voters []VoterPod `json:"voters"`; Identities map[quepaxa.NodeID]FenceTarget `json:"identities"`; SuspectedSince time.Time `json:"suspectedSince"`; Completions []time.Time `json:"completions,omitempty"`; Sequence uint64 `json:"sequence"`; Intent *autoIntent `json:"intent,omitempty"`; Completed *autoCompletion `json:"completed,omitempty"`; Learners []learnerIncarnation `json:"learners,omitempty"` }
type autoIntent struct { Supersedes string `json:"supersedes,omitempty"`; LearnerUID string `json:"learnerUID,omitempty"`; LearnerWAL string `json:"learnerWAL,omitempty"`; LearnerMissingSince time.Time `json:"learnerMissingSince"`; LearnerAttempt int `json:"learnerAttempt"`; Aborting bool `json:"aborting"`; RetirementSince time.Time `json:"retirementSince,omitempty"`; PreviousSpecHash string `json:"previousSpecHash,omitempty"`; NoQuorumSince time.Time `json:"noQuorumSince"`; Request FenceRequest `json:"request"`; Policy AutoPolicy `json:"policy"`; Durability string `json:"durability"`; Voters []VoterPod `json:"voters"`; Failed quepaxa.NodeID `json:"failed,omitempty"`; Proof *FenceProof `json:"proof,omitempty"`; RecoveryName string `json:"recoveryName"`; RecoveryUID string `json:"recoveryUID,omitempty"`; SpecHash string `json:"specHash,omitempty"`; LearnerName string `json:"learnerName,omitempty"` }
type autoCompletion struct { RecoveryName string `json:"recoveryName"`; RecoveryUID string `json:"recoveryUID"`; SpecHash string `json:"specHash"`; FenceProofID string `json:"fenceProofID"`; FenceRequestHash string `json:"fenceRequestHash"`; ClusterID string `json:"clusterID"`; Voters []VoterPod `json:"voters"`; At time.Time `json:"at"` }
type learnerIncarnation struct { SourceClusterID string `json:"sourceClusterID"`; OperationID string `json:"operationID"`; CredentialUID string `json:"credentialUID"`; Target FenceTarget `json:"target"`; Retirement *learnerRetirement `json:"retirement,omitempty"` }
type learnerRetirement struct { Request FenceRequest `json:"request"`; Proof FenceProof `json:"proof"` }
```

`time.Time` uses Go's standard JSON time encoding; fields marked `omitempty` are
omitted under the standard encoding rules.

## Policy contract

```go
type AutoPolicy struct {
  FailureGraceSeconds int64 `json:"failureGraceSeconds"`
  CooldownSeconds int64 `json:"cooldownSeconds"`
  MaxRecoveriesPerDay int `json:"maxRecoveriesPerDay"`
  AllowDataLoss bool `json:"allowDataLoss"`
}
type AutoObservation struct {
  Known bool; Voters int; Reachable int; QuorumVerified bool
  StoreAvailable bool; Durability string
}
type AutoDecision string
```

The decisions are `Healthy`, `Degraded`, `Suspect`, `Blocked`, and `Recover`.
`DecideAutoRecovery(policy, observation, now, suspectedSince, completions)` is
pure policy evaluation; controller code owns persistence and I/O. All three
policy numbers must be positive. Unknown observation, unavailable store, or an
invalid durability block. Verified quorum plus every voter reachable is
Healthy; verified quorum with misses is Degraded. No quorum with zero
`suspectedSince`, or elapsed time below grace, is Suspect. Past grace, async
without permission, at/above the rolling 24-hour completion count, or before
the most recent completion plus cooldown blocks; otherwise the result is
Recover. Exact messages/ordering are in
[automatic_policy.go](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/operator/automatic_policy.go).

## Fence request, proof, targets, limits

```go
type FenceTarget struct {
 NodeID quepaxa.NodeID `json:"nodeID"`; Pod string `json:"pod"`
 PodUID string `json:"podUID"`; WALIdentity string `json:"walIdentity"`
 NodeName string `json:"nodeName,omitempty"`; NodeUID string `json:"nodeUID,omitempty"`
 ContainerID string `json:"containerID,omitempty"`; ImageID string `json:"imageID,omitempty"`
}
type FenceRequest struct {
 LogicalID string `json:"logicalID"`; BindingUID string `json:"bindingUID"`
 Namespace string `json:"namespace"`; StatefulSet string `json:"statefulSet"`
 StatefulSetUID string `json:"statefulSetUID"`; SourceClusterID string `json:"sourceClusterID"`
 OperationID string `json:"operationID"`; Scope Scope `json:"scope"`; Targets []FenceTarget `json:"targets"`
}
type FenceProof struct {
 OperationID string `json:"operationID"`; SourceClusterID string `json:"sourceClusterID"`
 BindingUID string `json:"bindingUID"`; RequestHash string `json:"requestHash"`
 ProofID string `json:"proofID"`; ProcessesTerminated bool `json:"processesTerminated"`
 RecreationBlocked bool `json:"recreationBlocked"`; StorageQuiesced bool `json:"storageQuiesced"`
 Independent *IndependentEvidence `json:"independent,omitempty"`
}
```

The complete operator-owned external-attestation structures are:

```go
type IndependentEvidence struct { Version int `json:"version"`; Provider string `json:"provider"`; Mode string `json:"mode"`; ObservedAt string `json:"observedAt"`; RequestHash string `json:"requestHash"`; LedgerHash string `json:"ledgerHash"`; BarrierID string `json:"barrierID"`; Barrier EvidenceBarrier `json:"barrier"`; Runtimes []EvidenceRuntime `json:"runtimes"`; Storage EvidenceStorage `json:"storage"` }
type EvidenceBarrier struct { Kind string `json:"kind"`; ClosedOperationID string `json:"closedOperationID"`; ClosedRequestHash string `json:"closedRequestHash"`; CurrentPodUIDs []string `json:"currentPodUIDs"`; DeniedAt string `json:"deniedAt"` }
type EvidenceRuntime struct { NodeName string `json:"nodeName"`; NodeUID string `json:"nodeUID"`; InstanceID string `json:"instanceID"`; PodName string `json:"podName"`; PodUID string `json:"podUID"`; ContainerIDs []string `json:"containerIDs"`; ImageIDs []string `json:"imageIDs"`; RunIDs []string `json:"runIDs"`; WALIdentities []string `json:"walIdentities"`; State string `json:"state"`; RuntimeEvidence string `json:"runtimeEvidence"` }
type EvidenceStorage struct { Provider string `json:"provider"`; Endpoint string `json:"endpoint,omitempty"`; Bucket string `json:"bucket"`; Prefix string `json:"prefix"`; Account string `json:"account,omitempty"`; ConnectionStringDigest string `json:"connectionStringDigest,omitempty"`; QuietWindowSeconds int `json:"quietWindowSeconds"`; Before StorageSnapshot `json:"before"`; After StorageSnapshot `json:"after"`; Stable bool `json:"stable"` }
type StorageSnapshot struct { ObservedAt string `json:"observedAt"`; HeadGeneration string `json:"headGeneration"`; HeadMetageneration string `json:"headMetageneration"`; CurrentGeneration string `json:"currentGeneration"`; CurrentMetageneration string `json:"currentMetageneration"`; InventoryDigest string `json:"inventoryDigest"` }
type AttestationRequest struct { Request FenceRequest `json:"request"`; RequestHash string `json:"requestHash"`; LedgerHash string `json:"ledgerHash"`; StatefulSetUID string `json:"statefulSetUID"`; Runs []AttestationRun `json:"runs"`; CurrentPodUIDs []string `json:"currentPodUIDs"`; StorageScope AttestationStoreScope `json:"storageScope"` }
type AttestationRun struct { RunID string `json:"runID"`; NodeID string `json:"nodeID"`; PodUID string `json:"podUID"`; WALIdentity string `json:"walIdentity"` }
type AttestationStoreScope struct { Provider string `json:"provider"`; Endpoint string `json:"endpoint,omitempty"`; Bucket string `json:"bucket"`; Prefix string `json:"prefix"`; Account string `json:"account,omitempty"`; ConnectionStringDigest string `json:"connectionStringDigest,omitempty"` }
type AttestationResponse struct { Proof FenceProof `json:"proof"`; Independent IndependentEvidence `json:"independent"` }
```

`Scope` is exactly `Generation` or `Voter`; at most 64 targets are accepted.
The local `validateRequest` checks only nonempty operation ID, source cluster
ID, binding UID, a valid scope, and capacity; it leaves target shape and broader
binding checks to the fencing authority/executor. `Voter` scope cardinality is
checked by automatic execution when fencing a failed voter. `requestHash` is
deterministic JSON SHA-256 of the entire request, therefore includes the full
target array and is not an evidence/fence digest. Proof validation requires
matching operation/source/binding/request hash and proof ID, plus all of
`processesTerminated`, `recreationBlocked`, and `storageQuiesced` true. Pending
or partial responses remain errors. [fencing.go](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/operator/fencing.go)

`FencingClient` accepts HTTPS URL only: no user info, redirects do not forward
credentials, bearer token comes from a nonempty token file per request, and
response bodies are capped at 64 KiB. It rejects malformed/unknown JSON and
validates the returned proof. HTTP 202 is pending, not success.

Both `FencingClient.Fence` and `HTTPSAttestor.Attest` use `cloneClient`:
a nil injected client defaults to a 10-second timeout; for a nonnil client,
only `Timeout == 0` or `Timeout > 10*time.Second` is normalized to 10 seconds.
Positive timeouts up to and including 10 seconds remain unchanged. A negative
injected timeout also remains unchanged. Go 1.27.3's `http.Client.deadline`
creates a client deadline only when `Timeout > 0`, so the negative case supplies
no client-level timeout; the request's caller context still applies. It does
not guarantee an unbounded call, since the caller context or other configured
transport behavior can end it. The normal CLI configuration uses positive
10-second timeouts. The exported client-injection boundary therefore does not
provide an unconditional 10-second deadline.
Sources: [cloneClient](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/operator/fencing.go#L279),
[Fence call](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/operator/fencing.go#L345),
[Attest call](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/operator/fencing_attestor.go#L114),
[CLI configuration](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/cmd/rhiza-operator/main.go).
The Go 1.27.3 deadline predicate was confirmed by the parent from its local
GOROOT source; this is static evidence, not an executed timeout test.

## Independent attestation contract

`IndependentEvidence` version is 1. Its transport includes the attestation
request identity/closed scope, runtime observations, and storage observation.
The declared `QuietWindowSeconds` integer must be at least 30; this is not an
unconditional elapsed-time minimum because of the unchecked duration conversion
below. Attestation response bytes are capped at 512 KiB. Evidence must bind request
hash, ledger hash, target/current node scopes, and storage scope; runtime
identities must exactly match captured identities. Storage comparison checks
provider/location scope rather than exposing credentials. Missing, expanded,
or mismatched scope/identity evidence rejects the fence. Timestamp acceptance
is narrower than a general freshness or format check:

- Top-level `IndependentEvidence.ObservedAt` and `Barrier.DeniedAt` must be
  nonempty strings only. This validator does not parse them or compare their age.
- `Storage.Before.ObservedAt` and `Storage.After.ObservedAt` must parse with
  Go `time.Parse(time.RFC3339, ...)`. The declared `QuietWindowSeconds` must be
  at least 30. The exact elapsed-time acceptance predicate is
  `after.Sub(before) >= time.Duration(QuietWindowSeconds)*time.Second`.
  `QuietWindowSeconds` is a Go `int` with no additional upper-bound check, and
  conversion/multiplication into the signed 64-bit nanosecond `time.Duration`
  has no overflow check. On a 64-bit target, the multiplication can wrap, so
  acceptance does not always require either the declared elapsed seconds or
  even 30 elapsed seconds. For example, `QuietWindowSeconds=4611686018427387904`
  (`2^62`) passes the integer minimum but the multiplication wraps to zero;
  equal valid before/after timestamps can pass this duration predicate when
  the other evidence checks pass. This example is statically derived and was
  not executed. The value is outside the `int` range on a 32-bit target.
- The storage end time must not be later than `time.Now().Add(2*time.Minute)`;
  equality is accepted. There is no maximum past age for that window. The
  two-minute allowance is a future-end bound, not a symmetric freshness limit.
- Both storage snapshots must contain their generation/metageneration and
  inventory fields, `Stable` must be true, and their storage state must match.
  Separately, `independentFence` rereads the ledger and Pod inventory before
  persisting the proof; a changed ledger hash or inventory returns
  `ErrFencePending`. This stale-scope check does not impose observation-age
  validation.

These are current input-acceptance predicates, not a stronger freshness policy.
Sources: [evidence validation](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/operator/fencing_attestor.go#L221),
[storage timestamps](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/operator/fencing_attestor.go#L302),
[scope recheck](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/operator/fencing_executor.go#L563).

`HTTPSAttestor` requires a validated HTTPS endpoint, token-file, and CA-file
configuration. Its request key scopes idempotency to the snapshot; it does not
turn an old response into authority for a changed target scope. Cooperative
ledger state cannot be overwritten by independent attestation.

## Kubernetes I/O contract

`Kubernetes` has `BaseURL`, `Namespace`, `TokenFile`, `Client`, and deletion-scope
configuration. `NewInCluster` obtains in-cluster endpoint/token/namespace.
`Get`, `Put`, and `Post` encode/decode JSON through `request`; decoded JSON is
bounded at 16 MiB. Non-2xx responses produce `*APIError` with status only.
`DeletePod(namespace,name,uid)` and `DeleteSecret(...)` require a matching UID
precondition and allowed deletion scope. The operator distinguishes namespaced
fake HTTP behavior from proof that Kubernetes actually terminated the target.
[kubernetes.go](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/operator/kubernetes.go)

Kubernetes fencing creates/observes a `RhizaFence` resource. The request is
immutable on resume: changed identity/request cannot reuse an existing proof.
`FencingExecutor` authenticates the caller against process identity, verifies
supervised pod/learner binding, maintains a ledger at the source/UID key, and
returns transient authority errors without claiming a fence. Its `Reconcile`
processes fence CRs; it does not deploy workloads.

## Environment, archive, and anchor contracts

Environment is resolved in Kubernetes order: each `envFrom` ConfigMap/Secret
(Secret values base64-decoded) with prefix, then explicit `env`, including a
name-only entry as an explicit empty override. Dynamic `RHIZA_*` sources are
rejected except `RHIZA_NODE_ID` from `metadata.name`; `$(` expansion in any
Rhiza variable is rejected. Missing referenced data/key, invalid secret base64,
or unsupported dynamic source is an error.

The data contract rejects volume claim templates, nested data mounts,
subPath/subPathExpr, nonabsolute `RHIZA_DATA_DIR`, and any data volume other
than `emptyDir`. Object store identity compares provider, endpoint, bucket,
prefix, Azure account, and SHA-256 of an Azure connection string; default
provider `filesystem` is rejected for no-PVC recovery. The operator-wide
identity must be non-nil and match all fields exactly.

Anchor opt-in applies when either spec `anchorID` or
`RHIZA_RECOVERY_ANCHOR_ID` exists: they must match, reconfiguration must be
enabled, and an anchor client must exist. `AnchorClient.Activate(ctx, Request)`
and `Verify(ctx, Request, Receipt)` are the only controller anchor calls. The
Kubernetes backend stores `anchor.json` in a named ConfigMap and CASes its
resource version. An absent external anchor is failure, never initialization.

The immutable archive generation anchor establishes fork lineage; the external
application anchor separately controls live binding/generation activation.
`buildAnchorRequest` binds operation, source/target, membership, fork result,
and target generation-anchor hash, with `FenceHash` independently hashing the
fence assertion. `recoveryanchor.RequestHash` hashes the entire request,
including `FenceHash`; the fence digest is not the request hash. After record
validation, activation first returns the stored receipt for an exact
already-committed request. This predicate does not inspect `PendingWrite`:
an application update can leave a pending write while preserving the committed
receipt and live target binding/generation, so that exact retry can succeed.
Outside this fast path, starting or resuming an uncommitted activation rejects
pending application writes before reserving/resuming `Transition`, preserving
the application evidence. `PendingWrite` belongs to the ordinary application
write protocol, not recovery reservation.

A committed receipt must match the exact request hash, anchor/operation,
target, and live binding/generation, with no pending transition; `Verify` reads
the backend and compares the supplied receipt with its committed receipt.
`Verify` uses the same committed predicate and can therefore succeed while
`PendingWrite` is true. It does not establish absence of an application pending
write. Ordinary application `CheckBinding` separately rejects pending writes
and transitions and checks live binding/generation; receipt verification does
not substitute for that authority check.
In both `Starting` and `Restarted`, the controller reconstructs the request from
current operation/lineage state, requires equality with the persisted request,
and live-verifies the receipt. It repeats this verification immediately before
the successor StatefulSet PUT. A persisted receipt alone grants neither
activation authority nor cached authority for future application writes.
Sources: `pkg/operator/controller.go`, `buildAnchorRequest`,
`verifyAnchorRequest`, `startAndObserve`; `pkg/recoveryanchor/types.go`,
`RequestHash`; `pkg/recoveryanchor/coordinator.go`, `Activate`, `committed`,
`Verify`, and `CheckBinding`. See also the
[archive lineage contract](archive-recovery-go-reverse-spec-2026-10-30.md).

## Observable ordering and error status

Validation/status failures use phases including `Blocked`, `Recovering`, and
`AwaitingFence`; `ClusterStatus` is overwritten per status update with current
generation and persisted active values. Object writes that establish bindings,
membership, lineage, and control are immutable/CAS guarded. Status-only
rejections return the result of `record`/`autoStatus`: successful status write
returns nil, so `Run` does not log an error; status-write failure propagates.
Dependency, I/O, and CAS errors returned directly propagate to `Run` and are
logged while its context is live. Context cancellation may prevent a status
record.

Known gaps: this appendix does not reproduce dependency-owned `network`,
`recovery`, `recoveryanchor`, Kubernetes CRD manifest, or objstore wire schemas.
That exclusion does not apply to the operator-owned schemas above.
The direct operator-to-network recovery-probe wire boundary has its normative
owner in [HTTP recovery probe wire contract](request-http-go-contracts-2026-10-30.md#recovery-probe-wire-contract),
including the nested status schema, HMAC bytes, validation and deadlines.

Review: [corrected and independently rechecked within the recorded scope](operator-go-egm-review-2026-10-30.md).

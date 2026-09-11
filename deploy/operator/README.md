# Rhiza recovery operator

For Go or Rust applications embedding Rhiza, see the dedicated
[embedded application integration guide](../../docs/embedded-operator.md).

These manifests deploy a namespace-scoped controller for `RhizaRecovery`
and opt-in automatic recovery through `RhizaCluster`.
It monitors an **existing three-voter StatefulSet**. It does not create a
database, PVCs, or a new membership from scratch.

`no-pvc/` is a separate, render-only Kustomize input for the recovery
deployment contract. It contains only the selected voter StatefulSet and its
PDB, then removes `volumeClaimTemplates` and adds the `data` `emptyDir`:

```sh
kubectl kustomize deploy/operator/no-pvc
```

Review that rendered output alongside the existing `rhiza-config`,
`rhiza-object-store`, and headless Service manifests. It deliberately does
not include operator RBAC/Deployment or a Namespace, so rendering it cannot
silently widen permissions or select a different namespace. The Kustomize
input preserves `OnDelete`, parallel bootstrap, and the PDB.

Apply the namespace, CRD, RBAC, and Deployment in that order. The controller
uses its service account and the Kubernetes JSON API; it polls the namespace
selected by `--namespace`. The Deployment supplies that flag from
`POD_NAMESPACE`; if it is omitted, the command reads the mounted service
account namespace. `--poll-interval` defaults to `10s` and must be positive.

The operator reads the same `RHIZA_OBJSTORE_*` settings as Rhiza itself from
`rhiza-config` and `rhiza-object-store`. The object-store secret must provide
the bucket endpoint and credentials appropriate for the selected provider.
Generation-specific peer credentials are distinct secrets created by the
controller; they must not reuse the source voters' credentials.

## Observation and recovery requests

The supplied `sample-rhizarecovery.yaml` is observation-only because
`spec.recoveryID` is empty. Observation checks the source membership, live peer status, and the
no-PVC deployment contract but cannot change the StatefulSet.

A real request supplies a unique, explicit `recoveryID`, `statefulSet`, and
`sourceClusterID`. The controller derives the target generation from the
resource UID and that recovery ID. After a successor is reserved, do not edit its source, mode, or recovery ID,
and do not delete the resource: it is the resumable operation journal. Create
a separate resource for the next generation after this recovery completes.

`spec.durability` describes the desired target generation. Recovery policy is
always determined by the immutable **source** archive mode recorded by the
existing StatefulSet:

| Source mode | Required request policy |
| --- | --- |
| `before-ack` | The source archive must contain the certified prefix. `allowDataLoss` is not an acknowledgement that missing source history is safe. |
| `async` | `allowDataLoss: true` is required because acknowledged writes newer than the published source prefix can be absent. |

The controller best-effort archives reachable voters’ certified suffixes,
then reserves a single successor before StatefulSet changes. After the attested
fence, it permanently seals the authoritative archive HEAD
with CAS and validates the checkpoint (when present) and continuous suffix.
It finishes the verified target copy before deleting any source Pods. The source bucket must remain
available, immutable to late writers, and protected from rollback for the
entire recovery. A bucket copy or an apparently quiet pod is not proof that
the source is safe.

## Fence requirement

Only a trusted fencing authority may create or update recovery resource specs.
Grant that permission separately to your incident administrator or fencing
automation; do not grant it to ordinary application service accounts. The CR
attestation is an authorization boundary, not an independently verified power
or network fence.

For manual requests, `spec.fence` is a trusted out-of-band administrator attestation. It must bind
the same `recoveryID`, `sourceClusterID`, and live StatefulSet UID, set
`confirmed: true`, and contain concrete evidence. The operator never infers a
fence from missing pods, failed probes, or Kubernetes deletion state.

Before confirming it, fence **all** old voters, every client that can write,
and read replicas or learners that can reconnect with credentials. Prevent
late object-store writes from the old generation as well. This normally means
revoking old peer and client credentials, stopping external supervisors, and
blocking old workload identity or bucket access. Do not rely on scale-down
alone: a partitioned old voter can still retain voting authority.

## Deployment contract

The controller accepts only the no-PVC contract: database state is on an
`emptyDir` volume at `RHIZA_DATA_DIR` (default `/data`) without `subPath`.
This makes the replacement generation explicit. It is not a rolling upgrade:
after a valid fence and source capture, the old three-voter generation is
replaced as a whole. Never roll one new voter at a time into the old fixed
membership and do not reuse old PVCs.

The operator container has a read-only root filesystem and mounts an
`emptyDir` at `/tmp` for the recovery worker's temporary WAL files.

Local qualification used an isolated kind cluster and MinIO: two lost Pods
reported a missing quorum without automatic replacement; after runtime-confirmed
termination of the original voters, both async → before-ack and before-ack →
async recovery reached Complete, preserved archived SQL rows, and accepted new
writes. Unit tests cover interrupted status writes and credential reuse. This
qualifies the controller flow, not a production infrastructure fencing provider.

## Automatic recovery with Kubernetes fencing

Apply `cluster-crd.yaml` and `fence-crd.yaml` in addition to `crd.yaml`, then
apply the current RBAC. Enable `RHIZA_AUTOMATIC_RECOVERY=true` and
`RHIZA_FENCER_BACKEND=kubernetes` on the Operator. Adapt
[`automatic-example.yaml`](automatic-example.yaml) to the existing StatefulSet,
source generation and voter identities before applying it. All database voters
must opt into membership replacement.

The Operator persists the incident, checks the surviving quorum, creates an
immutable `RhizaFence`, and waits for matching completion evidence. A separately
authorized executor must terminate the complete requested identity scope,
prevent recreation and quiesce storage before updating the fence status. The
Operator service account cannot write that status. Missing executor or incomplete
proof leaves recovery pending; a Pod DELETE response is insufficient.

After proof, the Operator provisions and promotes a learner, fences and aborts a
lost learner before retrying a fresh identity, or restores a new generation when
quorum is lost. The StatefulSet remains the deployment basis. The kind executor
in `e2e/chaos/` is CI-only; production runtime/storage isolation still requires an
environment-specific executor. See the
[execution contract and CI evidence](../../docs/automatic-recovery-design.md#kubernetes-backend-실행-계약).

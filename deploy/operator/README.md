# Rhiza recovery operator

For Go or Rust applications embedding Rhiza, see the dedicated
[embedded application integration guide](../../docs/embedded-operator.md).

For the default provider-neutral cooperative executor, its startup contract,
installation, and hard-crash limitations, see
[Kubernetes fencing](../../docs/kubernetes-fencing.md). Installing the operator
alone still does not install a runtime fencing authority.

These manifests deploy a namespace-scoped controller for `RhizaRecovery`
and opt-in automatic recovery through `RhizaCluster`.
It monitors an **existing StatefulSet of at least three voters**. It does not create a
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

Apply the namespace, CRDs, RBAC, and Deployment in that order. The controller
uses its service account and the Kubernetes JSON API; it polls the namespace
selected by `--namespace`. The Deployment supplies that flag from
`POD_NAMESPACE`; if it is omitted, the command reads the mounted service
account namespace. `--poll-interval` defaults to `10s` and must be positive.

The manual terminal-attestation schema is installed separately with
`manual-recovery-attestation-crd.yaml`; its fencer read Role and the unbound
approver Role are in `fencing/manual-attestation-rbac.yaml`. Bind the approver
Role only to the independently authenticated incident-admin principal for the
installation. Do not bind it to the operator, fencer, application, or workload
service accounts. Installing these manifests alone does not enable manual
recovery: use this path only with a fencer release that issues and validates the
typed, expiring canonical challenge described below.

Post-fence standalone learner cleanup uses `--delete-namespaces`, defaulted from
`RHIZA_OPERATOR_DELETE_NAMESPACES`. Empty means the actual operator Pod namespace
(verified from the mounted service-account namespace, independently of the watch
flag); a CSV allowlist or exclusive `*` opts into broader eligibility. This does
not expand the watcher or grant RBAC. Default RBAC stays namespace-scoped. See
[Pod deletion scope](../../docs/kubernetes-fencing.md#pod-deletion-scope).

The operator reads the same `RHIZA_OBJSTORE_*` settings as Rhiza itself from
`rhiza-config` and `rhiza-object-store`. The object-store secret must provide
the bucket endpoint and credentials appropriate for the selected provider.
Generation-specific peer credentials are distinct secrets created by the
controller; they must not reuse the source voters' credentials.

The independent NoPVC mutation profile requires a configured remote canonical
authority store whose constructor accepts the selected ObjectStorage
provider's conditional create and compare-and-swap capabilities; operations
that retire exact objects also require that provider's versioned-delete
semantics. It also requires the runtime-agent CA and exactly one source-writer
credential mode: the existing
`--source-writer-credentials-file` (or
`FENCER_SOURCE_WRITER_CREDENTIALS_FILE`), or explicit
`--source-writer-use-adc` (or `FENCER_SOURCE_WRITER_USE_ADC=true`). These
options are mutually exclusive; ADC mode is accepted only with the independent
profile and has no fallback to the credential file. The current source-mutation
proof adapter is GCS-specific; selecting another provider for source mutations
fails startup with an explicit unsupported-profile error before Google client
setup. This does not make GCS a requirement of the general ObjectStorage
architecture or establish support for another mutation provider. ADC mode uses
the fencing authority Pod's Application Default Credentials for the GCS source
writer (on GKE, configure that Pod's Workload Identity principal deliberately).
When the authority-state provider is GCS and also uses default credentials, it
uses that principal too; explicit `RHIZA_OBJSTORE_SERVICE_ACCOUNT` credentials
can select a different GCS state principal. ADC mode does not itself prove
separation between authority-state and source-object permissions. Keep the source
application's KSA distinct and read/list-only, do not mount or inject writer
credentials into the application Pod, and verify IAM bindings and denied
operations with the qualification harness before making an isolation claim.
Source staging and execution receipts then use root-bound remote ObjectStorage;
local stage and receipt directories are rejected rather than used as
fallbacks. The remote stage quota is 64 tickets and 4 GiB per producer.
Cancellation before a ticket enters receiving may durably retire it and release
quota. Once receiving/admission has begun, tickets remain reserved until their
exact mutation is found in verified compacted archive history; age alone never
frees capacity.

## Operator image

Published GitHub releases build and push `ghcr.io/mrchypark/rhiza-operator`
with the release tag (for example, `v0.14.1`). Stable releases also update
`latest`; prereleases do not. The image currently targets `linux/amd64`.
Pull requests validate the same Docker build without publishing.

Set the Deployment image to the versioned GHCR image or its digest instead
of the sample's local `rhiza-operator:latest` name. If the package is private,
configure an `imagePullSecrets` entry with package read access. Release
publishing does not install anything in your Kubernetes cluster and does not
upload standalone CLI binaries.

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

## Fence requirement and manual terminal evidence

Normal recovery requires the canonical fencer proof and all existing source,
storage, quorum, and read-barrier checks. Kubernetes Pod absence, failed probes,
StatefulSet scale-down, and a `spec.fence.confirmed` boolean are not terminal
proof.

For the explicitly selected human-recovery exception, the approval is a
separate immutable `RhizaManualRecoveryAttestation`, not a field in
`RhizaRecovery.spec`, status, or an operator request. Its authorization comes
from Kubernetes RBAC and API audit: the installation binds
`rhiza-manual-recovery-approver` only to its incident-admin identity; the
operator has no approval write access; the fencer has namespace-scoped `get`
only (no list or write verbs), and its implementation must fetch the exact
deterministic object name and verify its UID and spec. The `attester` text field
is descriptive and is not proof of identity.

The fencer must first establish canonical CONTROL closure, the complete
historical cohort, exact archive/provider scope, target binding, and current
Pod UID inventory. Only then may it return a typed, expiring challenge bound to
the Recovery name and UID, operation, full policy (including
`allowDataLoss`), closed root/ledger/cohort, storage scope, archive, target,
and current Pod inventory. The administrator must independently verify every
old run is terminated, recreation is blocked, old peer/client write access is
blocked, old archive-writer access is revoked, and storage is quiescent. The
fencer rechecks those bindings and challenge expiry both when accepting the
immutable approval and when consuming its proof. Any drift, expired challenge,
or changed approval object fails closed. Before a canonical source-successor
claim exists, an expired challenge requires a new Recovery operation. After
that exact claim has consumed the accepted immutable approval, the same
Recovery may resume after expiry only while its current source, root, workload,
inventory, challenge, approval UID/spec, and claim still match exactly.

This exception is an explicit external-administrator trust decision, not
IndependentV2 runtime evidence and not a guarantee from Kubernetes object
absence. It cannot synthesize a missing ACK or archive HEAD; an async source
still requires the existing `allowDataLoss: true` gate and recovers only the
canonical sealed tip. For a `before-ack` source, the same canonical archive-tip
and closure checks apply, but manual recovery does not assert data loss and does
not require `allowDataLoss`. A surviving source quorum should use normal membership
recovery unless an authorized operator deliberately chooses full-generation
recovery.

The schema and RBAC manifests do not enable the route by themselves: deploy a
matching controller and fencer that implement the typed challenge and canonical
attestation-consumption protocol. Do not create an attestation or treat existing
`spec.fence` fields as equivalent.

## Deployment contract

The controller accepts only the no-PVC contract: database state is on an
`emptyDir` volume at `RHIZA_DATA_DIR` (default `/data`) without `subPath`.
This makes the replacement generation explicit. It is not a rolling upgrade:
after a valid fence and source capture, the old generation is
replaced as a whole. Never roll one new voter at a time into the old fixed
membership and do not reuse old PVCs.

The operator container has a read-only root filesystem and mounts an
`emptyDir` at `/tmp` for the recovery worker's temporary WAL files.

The supervised StatefulSet patch deliberately omits the `rhiza-object-store`
Secret from the source Pod. Put only non-secret GCS coordinates (`RHIZA_OBJSTORE_PROVIDER=gcs`, bucket, and prefix) in `rhiza-config`; bind the `rhiza-supervised` workload identity to read/list-only access. The mutation writer credential file belongs only to the fencing authority deployment. Do not mount or inject that credential into the source Pod.

## Voter count

`spec.voterCount` is the numeric voter count for the source generation and for
the replacement generation; it must be between 3 and 64. The StatefulSet must
declare exactly that many desired replicas and use Pod names
`<statefulSet>-<ordinal>`. The controller derives those node IDs and Pod names,
the headless-service DNS names from `spec.serviceName` and the operator
namespace, and fresh voter credentials when needed. Optional `httpPort` and
`peerPort` default to 8080 and 9090. When the watched StatefulSet already exports
`RHIZA_CLUSTER_MEMBERS`, the controller adopts it without regeneration and
requires its count and node IDs to match the derived inventory. When the workload
exports no membership, fresh bootstrap is disabled unless `spec.bootstrap: true`
is set explicitly. A bootstrap also refuses to run when the adopted cluster ID
already has a `voters/membership.json` commitment in object storage. When allowed,
the controller reserves one operation-owned candidate while `replicas: 0`, stores
the bootstrap identities in one immutable Secret, installs the Secret references
in the template, and only then scales to the declared count. Existing Pods block
writer-free bootstrap; a template update alone is not treated as success.
`spec.clusterDomain` controls the DNS suffix used in
generated URLs and defaults to `cluster.local`. Before patching, the controller
also verifies that the workload exports `RHIZA_NODE_ID` through the downward API,
that `RHIZA_BIND_ADDR` uses `httpPort`, and that `RHIZA_PEER_ADDR` uses `peerPort`.
The Service named by `spec.serviceName` must exist and be headless. The controller rejects a count that
disagrees with replicas or with an exported member list, naming both numbers.
Even counts are supported, because QuePaxa requires only a strict majority of the
configured membership for quorum and completion. The count cannot be changed in
place: use a new generation or change one voter at a time through
reconfiguration. Generation fencing must name every voter plus the retained
learner history, and `RhizaFence.spec.targets` admits 64 of them. Automatic
recovery refuses to bind a count longer than that and blocks before reserving or
starting a fence whose scope exceeds the bound, naming both numbers in either
case.
A recovery is declared `Complete` only when the target StatefulSet declares
exactly that many desired replicas and every configured voter's identity was
authenticated first; a scaled-down target stays `Blocked` or `WaitingReady`
instead of becoming the next generation.

## Upgrading an existing automatic binding

Existing `RhizaCluster` objects need `spec.voterCount` added and set to the
StatefulSet's desired replicas and the exported `RHIZA_CLUSTER_MEMBERS` count. The
control record then migrates on the next reconcile, once the legacy binding hash
and those observable counts all match. If any check fails, the controller blocks
and names the failing condition instead of accepting a different workload.

The release note for this change must disclose both the CRD field change
(`voterPods` is replaced by the required numeric `voterCount`) and the automatic
control-record migration that rewrites a legacy binding hash to the current
binding version.

Local qualification used an isolated kind cluster and MinIO: in the three-voter
deployment, two lost Pods reported a missing quorum without automatic
replacement; after runtime-confirmed
termination of the original voters, both async → before-ack and before-ack →
async recovery reached Complete, preserved archived SQL rows, and accepted new
writes. Unit tests cover interrupted status writes and credential reuse. This
qualifies the controller flow, not a production infrastructure fencing provider.

## Automatic recovery with Kubernetes fencing

Apply `cluster-crd.yaml` and `fence-crd.yaml` in addition to `crd.yaml`, then
apply the current RBAC. Enable `RHIZA_AUTOMATIC_RECOVERY=true` and
`RHIZA_FENCER_BACKEND=kubernetes` on the Operator. Adapt
[`automatic-example.yaml`](automatic-example.yaml) to the existing StatefulSet,
source generation and voter identities, and set `spec.automatic: true` only after
configuring the executor. The sample defaults to observation. All database voters
must opt into membership replacement.

The Operator persists the incident, checks the surviving quorum, creates an
immutable `RhizaFence`, and waits for matching completion evidence. A separately
authorized executor must terminate the complete requested identity scope,
prevent recreation and quiesce storage before updating the fence status. The
Operator service account cannot write that status. Missing executor or incomplete
proof leaves recovery pending; a Pod DELETE response is insufficient.

When the Operator uses the HTTPS fencer client, it posts the immutable request
to `/v1/fences` on `rhiza-fencer`. The adapter authenticates the configured
controller service account and creates or joins the same `RhizaFence` CRD record;
HTTP 202 means the executor has not yet stored a validated proof. The CRD remains
the only request journal.

For a certified zero-tip source, recovery uses the same explicit `RhizaRecovery`
operation and additionally requires the fencer to register a canonical empty
target authorization. This path is distinct from a fork: it records tip zero
and an immutable empty-HEAD receipt, then waits for a target quorum plus an
authenticated ReadIndex and linearizable application read before completion.
An async source requires `allowDataLoss: true`; without it, the operation stays
blocked. The fencer Role needs only `get` on `rhizarecoveries` to verify the
persisted operation and policy; it does not gain recovery-resource write access.

After proof, the Operator provisions and promotes a learner, fences and aborts a
lost learner before retrying a fresh identity, or restores a new generation when
quorum is lost. The StatefulSet remains the deployment basis. The kind executor
in `e2e/chaos/` is CI-only; production runtime/storage isolation still requires an
environment-specific executor. See the
[execution contract and CI evidence](../../docs/automatic-recovery-design.md#kubernetes-backend-실행-계약).

Dead-Pod completion requires enrolled runtime and mutation evidence v2. Legacy
quiet-window-only attestations are rejected by both authority and recovery
controller; setting any `RHIZA_FENCE_ATTESTOR_*` value now fails command startup
instead of advertising unsupported healing. The v2 reconciliation and retained
history path has deterministic fake-provider coverage, but `rhiza-fencer serve`
does not yet configure real runtime/mutation providers. It must not be advertised
as production dead-Pod healing. No node, CRI or cloud credential is added to the
operator by this change. See
[Independent evidence](../../docs/kubernetes-fencing.md#independent-evidence-for-an-unreachable-incarnation).

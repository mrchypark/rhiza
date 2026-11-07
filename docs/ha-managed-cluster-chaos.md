# Managed HA Chaos Mesh: qualification scopes

## Automated main qualification

[Main run 42176644960](https://github.com/mrchypark/rhiza/actions/runs/42176644960)
qualified commit `2262531f266f2638eb21baea6f65de45465ffe90` in six lanes:
internal request/SQL+KV and Graph, each with S3, GCS and Azure adapters. The
`HA managed kubelet bootstrap` workflow runs the shared fixture on isolated Kind
clusters with pinned Floci-family emulators and actual provider SDKs. This is
not live-cloud parity. Local development uses VersityGW; neither path introduces
a MinIO installation procedure.

The fixture checks two sequential fresh-Pod replacements, followed by a
same-Pod container restart, certified identity/membership transitions and typed
SQL/KV/Graph C ABI ACK readback. The second replacement targets the first new
Pod identity. These are healthy-quorum repair and restart qualification, not
whole-quorum generation DR or infrastructure fencing. Current ownership and
deployment roles are in the [operator guide](../deploy/operator/README.md).

A Pod deletion response or API absence does not prove process termination,
recreation prevention or completion of outstanding storage work. Independent
DR still requires those conditions, a source covering successful ACKs and live
activation authority. Automatic generation DR is not implemented or qualified.

## Separate opt-in manual live-cluster qualification

The manual `HA managed Kubernetes Chaos Mesh` workflow is separate from the
six-lane run above. It does not rebuild the application image. All modes use a
disposable namespace and an in-namespace S3 emulator, not live GCS or Azure:

- `release` uses the published tag's harness and the immutable release operator
  image with matching source revision. The tag must contain the reference-host
  manual evidence path; older tags such as v0.23.2 are refused before cluster
  execution rather than paired with a different harness revision.
- `candidate` uses the exact protected-main source and matching candidate
  operator image. Both this mode and `release` use the virtual-cluster driver
  described below.
- `candidate-direct-host` uses the exact protected-main source and matching
  `managed-ffi-contract` candidate image in a pre-created host namespace. It
  requires the preinstalled v1alpha2 RhizaCluster API and does not install CRDs
  or use vcluster. It is not qualification of the published operator image.

The vcluster modes check reference-host SQL, not Rust/C ABI. The direct-host
mode retains its Rust/C ABI checks. Manual modes use scoped Chaos records and
Pod API observations, not the Kind workflow's host CRI proof.

The administrator creates a fresh `rhiza-ha-chaos-*` namespace and short-lived
namespace-only Kubernetes service account. Upload its kubeconfig as temporary
repository secret `RHIZA_CLUSTER_CHAOS_KUBECONFIG`; supply its context, namespace,
namespace UID and `rhiza-chaos-runner` ServiceAccount UID to the workflow. Do not commit credentials, cluster addresses or personal
cloud configuration. The administrator must delete this secret and the owned
namespace/RBAC after the terminal run and verify absence. Existing shared Rhiza
CRDs and objects must be snapshotted before/after by the administrator, not read
using an overprivileged CI credential.

The namespace credential needs only its namespace's pods (including logs,
exec and port-forward), services, endpoints, secrets, configmaps, serviceaccounts,
events, deployments, replicasets, statefulsets, jobs, roles/rolebindings and
leases, plus PodChaos objects. Creating delegated namespace Roles requires the
corresponding permissions in that namespace. The CI guard reads the exact runner
ServiceAccount UID: a recreated namespace cannot recreate that original UID or
reuse its token. The administrator separately verifies namespace UID before and
after the run. No cluster-scoped Namespace GET grant is needed.
No permission to create Namespace, Node, CRD, ClusterRole,
IAM, storage buckets or other namespaces is permitted. Auth expiry must cover
the workflow's 20-minute ceiling and cleanup, but not exceed one hour.
Direct-host requires exclusive use of the dedicated namespace for the run. It
preflights fixture names, creates its overlay without adopting existing objects,
and uses the returned UIDs as deletion preconditions for the overlay and CR.
Unconfirmed overlay or CR creations are left for the administrator. Cleanup of
the other exact, preflighted fixture names depends on exclusive namespace use. The
direct-host projection removes unused Recovery API rules; the existing runner
must already be allowed to delegate the remaining namespace rules. Do not grant
`escalate` or `bind` to bypass an RBAC refusal.

The parent verifies the namespace UID; the driver verifies the original
ServiceAccount UID and namespace emptiness, then installs the OSS
vcluster chart **0.28.0**, SHA256
`d65fa9a49dc423e62377fde56e5cbec727d3260d2dd17acfdca3bcc6d58110e4`.
ClusterRole generation, real-node sync, persistent claims and hostname rewrite
initContainers are disabled. The control plane uses ephemeral storage with
bounded resources. Rhiza v1alpha2 CRDs exist only in this virtual API; the shared
host CRDs are not applied or patched. No privileged, hostPID, hostNetwork or
hostPath resource is permitted in the rendered chart. No new nodes are created.
The administrator supplies one existing Ready node's verified hostname label.
Control-plane/CoreDNS chart settings and OSS syncer
`sync.fromHost.nodes.selector.labels` pin every physical workload Pod to that
hostname, including Deployments created internally by the operator. Real-node
sync remains disabled. This independent pinning is implemented in v0.28.0
`pkg/controllers/resources/pods/syncer.go:65–70,223–234`; it does not use paid
translation patches or unsupported RhizaCluster fields. Mapped host voters are
checked again.
An unavailable pinned node causes pending/failure, not scheduling on new nodes.
The parent creates a namespace ResourceQuota and LimitRange before dispatch to
bound Pod count, CPU and memory for containers without explicit requests.
The administrator must set and monitor a bounded runtime and cost budget; the maximum is not a spending target.

The existing managed bootstrap fixture is shared with Kind qualification. Its
operator, init and main containers all use the digest verified for the selected
mode. Storage is
the disposable in-namespace pinned Floci S3 adapter, not MinIO or live GCS.
Actual SDK creation uses the existing Go helper through a loopback port-forward;
it has no ambient cloud credential fallback.

For each fault the driver matches the virtual Pod's exact name, namespace and UID
against the vcluster host annotations, requiring exactly one ready host Pod.
Chaos Mesh targets only that host namespace/name and virtual-UID annotation.
Direct-host checks the exact Pod UID before submitting a namespace/name-scoped
fault; this check is not atomic with Chaos selection or proof of physical
termination.
It records host UID/containerID and virtual Pod observations; names are not
guessed. Two sequential `pod-kill` actions must each result in a new Pod UID
and Completed certified repair; the second targets the first replacement.
There is no fallback virtual Pod deletion to manufacture that outcome.
Then `container-kill` must increase restartCount in the second replacement Pod
without changing its UID, repair origin, adoption or voter identity hashes. All three actions
require `AllInjected=True` and exactly one matching injected container record,
followed by the observed runtime turnover. These kill actions are one-shot:
the driver records and deletes each fault object and verifies its absence
before checking application recovery. It does not require or claim a controller
`AllRecovered=True` condition. Controller condition names are not a substitute
for application recovery: ACK readback, fresh writes, two completed
initContainers and NoPVC remain required.

Fault removal, Helm uninstall, host Pod/fault absence and private credential /
port-forward cleanup are checked even on failure. Raw first-failure evidence is
retained in the Actions artifact; an exit code alone is not qualification PASS.
The parent administrator remains responsible for final namespace, temporary
secret and RBAC removal and checking the original shared CRD/object snapshots.

This is not live GCS, network-partition, physical fencing, whole-quorum disaster
recovery or performance qualification. Missing/ambiguous evidence fails closed.

vcluster mapping and deletion semantics are pinned to the upstream v0.28.0
`pkg/util/translate/types.go`, `translate.go`, and
`pkg/controllers/resources/pods/syncer.go`: a physical Pod disappearance after
start deletes the virtual Pod instead of silently recreating the same identity.
The runtime oracle still requires this outcome rather than assuming it.

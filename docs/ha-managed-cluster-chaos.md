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

The manual `HA managed Kubernetes Chaos Mesh` workflow is a separate driver for
the published release and its immutable operator image. The automated six-lane
run above is not evidence that this manual workflow ran. Run the manual driver
only after the release image has published; it does not rebuild an application
image or substitute a different harness revision. Its scope below uses a
disposable namespace and local S3 emulator, not live GCS or Azure storage.

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
runtime, init and main containers all use the exact released digest. Storage is
the disposable in-namespace pinned Floci S3 adapter, not MinIO or live GCS.
Actual SDK creation uses the existing Go helper through a loopback port-forward;
it has no ambient cloud credential fallback.

For each fault the driver matches the virtual Pod's exact name, namespace and UID
against the vcluster host annotations, requiring exactly one ready host Pod.
Chaos Mesh targets only that host namespace/name and virtual-UID annotation.
It records host UID/containerID and virtual Pod observations; names are not
guessed. `pod-kill` must result in a new virtual Pod UID and Completed certified
repair. There is no fallback virtual Pod deletion to manufacture that outcome.
Then `container-kill` must increase restartCount in the same replacement Pod and
preserve repair origin, adoption and voter identity hashes. Both experiments
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

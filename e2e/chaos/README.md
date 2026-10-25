# Operator recovery chaos test

The `operator-chaos` CI job builds both binaries from the candidate revision and
runs them with the pinned Versity Gateway S3 fixture in an ephemeral Kind
Kubernetes cluster. It applies the repository CRD and namespace-scoped RBAC and
drives the running Operator through `RhizaRecovery` resources.

The GCS post-release qualification installs admission policies only for its
scoped Chaos Mesh fault resources. It intentionally excludes a runtime Pod
admission policy and therefore makes no claim that it prevents an otherwise
authorized actor from creating Pods. Runtime qualification instead retains
exact workload identity, image, node, security-shape, RBAC, and default-deny
NetworkPolicy checks.

Inspector TCP network-isolation proof is required by default. Only an explicitly
user-approved scope exclusion may set
`RHIZA_INSPECTOR_NETWORK_ISOLATION=exclude-user-approved` for that execution;
`required` is the other valid value. Empty or unknown values fail before execution.
The opt-in does not change cluster settings, remove the deny-all policy, or relax
Inspector identity, RBAC, token, credential, image, or policy-shape checks. GCS
scope, HEAD/CURRENT freeze, cold object-store recovery, read barriers, and Chaos
fault/readback checks remain required.

An excluded execution records `EXCLUDED` in `inspector-network-isolation.txt`.
Only after the remaining checks succeed does `result.txt` report `SCOPED PASS`
with the explicit exclusion; it is not network-isolation or full qualification
PASS. Result consumers must preserve that scope and must not turn native exit 0
alone into an unconditional qualification PASS. Without the opt-in, the existing
paired reachable-control/denied-Inspector/reachable-control proof is unchanged.

The workload retries stable request IDs during faults. The test checks:

- SIGKILL with the same Pod and WAL preserves voter identity.
- Pod deletion destroys emptyDir; the old voter identity refuses to restart.
- A fenced voter is replaced while the learner's QUIC ingress is dropped with
  iptables. Restarting the Operator preserves its journal; restoring traffic
  allows promotion and writes through the promoted voter.
- A second replacement includes the previously promoted standalone voter.
  Deleting the pending learner requires explicit abort; a fresh learner can
  complete the same recovery operation.
- Stopping every current voter and preventing StatefulSet scheduling allows a
  fenced whole-generation recovery. All acknowledged rows and request deduplication
  survive, and the new generation accepts writes.

Fence approval is explicit. The harness first verifies CRI process termination
and prevents source workload recreation, then submits the administrator
attestation. This does not implement an infrastructure fencing provider.

CI uploads `evidence.json`, Kubernetes objects, Pod logs and kind diagnostics even
on failure. A final `PASS` event is required; intermediate events alone do not
establish complete recovery. This is a single Kubernetes-node test of real
process, storage-loss and network faults, not physical-host or multi-node network
partition qualification. It makes no performance claim.

To rerun against an isolated kind cluster named `rhiza-operator-chaos`, load the
test images `rhiza-chaos:local` and `rhiza-operator-chaos:local`, then run:

```sh
python3 -u e2e/chaos/operator_recovery.py
```

The CI workflow contains the exact image and cluster setup commands. The harness
creates namespace `rhiza-chaos`; delete the disposable cluster after collecting
results. Do not run it against a shared cluster.

## Anchor Kind contract scope

The `anchor-operator-chaos` Kind job currently reports only
`SUPPORTED-NEGATIVE`. Its valid anchor ConfigMap and anchor service are paired
with the fixture's Versity Gateway/S3 archive and Kubernetes cooperative
fencer. That fencer does not implement canonical archive-fork registration, so
the Operator must block before anchor activation, target reservation,
StatefulSet mutation, or source-archive mutation. The harness checks those
invariants and does not emit a recovery `PASS` event.

This negative contract is not anchor recovery qualification. A positive anchor
recovery scenario remains a follow-up for a real-GCS path that supplies the
canonical authority store and verified archive-fork evidence; the current S3
Kind fixture cannot claim that result.

## GCS post-release qualification

The GCS/Chaos harness has no cloud defaults. Keep project, cluster, bucket,
service-account, node, context, local path, and object-prefix values outside the
repository. Never commit private execution receipts or a populated environment
file.

Every run requires explicit values for `RHIZA_GCP_PROJECT_ID`,
`RHIZA_GCP_PROJECT_NUMBER`, `RHIZA_GKE_CLUSTER`, `RHIZA_GKE_LOCATION`,
`RHIZA_KUBE_CONTEXT`, `RHIZA_GCS_BUCKET`, `RHIZA_GCS_PREFIX`,
`RHIZA_NAMESPACE`, `RHIZA_DATA_GSA_ID`, `RHIZA_NODE_A`, `RHIZA_NODE_B`,
`RHIZA_NODE_C`, `RHIZA_HOST_IMAGE`, and `RHIZA_METADATA_IMAGE`. The prefix,
namespace, and data service-account ID must contain the fresh eight-hex-digit
`RHIZA_RUN_ID`; images must be immutable digests. Administrative execution also
requires the reviewed generator path and digest, a fresh private absolute
`RHIZA_AUTH_STATE`, the expected active account, explicit `I_APPROVE_NEW_SCOPE`,
and the fixed KRW 10,000 safety ceiling. The ceiling is a limit, not a spending
target. Use a fresh namespace and prefix and grant only the temporary
project/cluster metadata and exact-prefix object permissions checked by the
harness. Local bootstrap also requires `RHIZA_HARNESS_SHA`, set to the exact
reviewed commit planned for this run. Federation uses the exact protected
`RHIZA_WORKFLOW_SHA` instead. Runtime render and execution require a fresh
absolute `RHIZA_OUTPUT` and `RHIZA_AUTH_CREATION_SHA`, the exact commit that
created the reviewed authorization contract.

The voterCount launcher also requires `RHIZA_KUBECTL_IMAGE`, an immutable digest
for an image that provides `/bin/sh` and `kubectl`. During `install`, it verifies
the three named nodes are distinct and Ready, records the exact set with the
run's namespace UID, and applies required node affinity to the voter template,
operator, fencer, runtime-agent DaemonSet, inspector pair, IAM probes, and expiry
Job. Operator-created voter replacements inherit the StatefulSet affinity. A
later `run` is refused if the namespace UID or selected node set changed. This
pins placement to existing nodes; it does not reserve their capacity or prevent
unrelated cluster autoscaling.

Install is host-deadline limited to 35 minutes; run is limited to 60 minutes
(the collector's existing 45-minute deadline is unchanged). GNU `timeout` sends
TERM and allows up to ten minutes for the cleanup trap before KILL. Host cleanup
is best effort and cannot survive host loss or SIGKILL. A run-owned expiry Job
starts before workload manifests, uses one 7,200-second epoch captured when
the manifest is rendered (so a replacement Pod cannot restart the wait), then deletes only the
exact namespace after rechecking its captured UID, resourceVersion, and run
label. It sends Kubernetes `DeleteOptions` using kubectl's raw DELETE route,
with both UID and resourceVersion preconditions; it does not rely on kubectl's
unsupported `--preconditions` flag. Its ClusterRole permits only `get` and
`delete` for that exact namespace. The pinned image must provide `kubectl`,
`date`, `sleep`, and `/bin/sh`. Its epoch is generated by the installer host
and compared with the Pod node's clock; clock skew can move the expiry earlier
or later, while the Job's active deadline remains the upper bound;
the generated cluster-scoped RBAC objects carry an owner reference to the same
namespace UID. `namespace-cleanup.status` records early deletion only after
absence is observed. If the runtime credential cannot delete namespaces, the
status records that cleanup is pending until the in-cluster expiry attempt, and
an unverified local cleanup makes the launcher exit nonzero. The
Job has a 10,800-second active deadline. Kubernetes/API unavailability,
unschedulable captured nodes, image-pull failure, finalizers, or garbage
collector failure can still prevent or delay cleanup; expiry is not absolute.
The namespace quota allows 12 Pods for the three agents, core workloads, expiry
worker, probes, inspectors, and one temporary learner.

Local-direct execution does not require GitHub credentials. Optional CI
federation additionally requires `RHIZA_GITHUB_REPOSITORY`, its numeric
repository and owner IDs, an exact workflow ref at the protected SHA, a protected
environment, and a fresh CI service-account ID. The bootstrap verifies the GCP
project ID/number and, in federation mode, the repository/owner IDs and workflow
file before creating cloud resources.

Cluster discovery requests portable full JSON and streams it directly through a
strict field projection. Raw cluster metadata, CA bodies, client credentials,
tokens, and command-based auth fields are never written to receipts or logs;
receipts contain only the selected endpoint, CA hash, cluster/location, node-pool
metadata, or a generic failed stage and native exit code.

With those variables exported, including fresh `RHIZA_OUTPUT` and the planned
exact SHA inputs, review the offline render first:

```sh
sh e2e/chaos/bootstrap-gcs-postrelease.sh render-local > /tmp/rhiza-auth-review.yaml
sh e2e/chaos/run-gcs-postrelease.sh render
```

After reviewing scope, receipts, digests, expiry, and cost cap, direct execution
uses `apply-local`, `mint-local`, and `run-local`; federation uses `apply` and
`run`. These are one-shot operations: do not reuse an output or receipt
directory, adopt colliding resources, or rerun after an unknown outcome.

## voterCount operator path (GCS + Chaos Mesh)

`run-votercount-gcs-chaos.sh` is the launcher for the v0.19.5 voterCount operator
path. It is a second, run-owned path that shares the bootstrap above: run
`bootstrap-gcs-postrelease.sh apply-local` first so the namespace, the `rhiza-gcs`
and `rhiza-operator` workload-identity bindings, the quota and the admission
policies exist. The control-plane writer identity is `RHIZA_DATA_GSA_ID`; the
operator and fencer use it because they manage recovery and authority objects.
Configure its Workload Identity binding for both KSAs `rhiza-operator` and
`rhiza-fencer` in the run namespace, with the control-plane bucket permissions
needed by those components. Set `RHIZA_READONLY_WORKLOAD_GSA_ID` to a different,
fresh GSA and bind it to KSA `rhiza-workload` with object viewer access only on
`${RHIZA_GCS_PREFIX}data/`. Do not grant it access to the parent prefix or the
private `${RHIZA_GCS_PREFIX}control` authority prefix. The launcher rejects
using the same GSA for both identities. No service-account keys are generated
or mounted.

The launcher keeps the supplied `RHIZA_GCS_PREFIX` as the run-owned parent,
places application data under its `data/` child, and puts canonical authority
state under its `control` child. Keep control-prefix objects out of public
evidence artifacts. The runtime-agent DaemonSet reserves host port 9445 on each
eligible node, so qualification runs sharing a node set must be serialized.

Before `install`, provide the existing runtime-agent CA and TLS Secrets through
`RHIZA_RUNTIME_AGENT_CA_SECRET` and `RHIZA_RUNTIME_AGENT_TLS_SECRET`. The CA
Secret must contain `ca.crt`; the TLS Secret must contain `tls.crt` and `tls.key`.
Also supply the exact qualified node values through `RHIZA_CONTAINERD_VERSION`,
`RHIZA_CONTAINERD_RUNTIME_TYPE`, `RHIZA_CGROUP_PROFILE_ID`, and
`RHIZA_KERNEL_RELEASE`. These are required inputs, not proof of qualification:
the launcher renders the shipped runtime-agent DaemonSet with those declarations
and pins its authority to the installed fencer Deployment UID; runtime
enrollment must still measure and verify the actual nodes. Missing or placeholder
inputs fail closed.

For the native `run-gcs-ha-dead-pod` passive terminal-proof scenario, set
`RHIZA_RUNTIME_AGENT_PASSIVE_KERNEL=yes` and
`RHIZA_QUALIFIED_SANDBOX_EXECUTABLE_SHA256` during `install`. The digest must be
independently approved; the launcher rejects the privilege opt-in without it.
The base DaemonSet always drops all capabilities. The explicit test-namespace
overlay adds only `SYS_PTRACE`, keeps privilege escalation and privileged mode
disabled, and pins `passive-kernel-v1` in the Pod template. The installed profile
binds the digest and exact fixed argv `["/pause"]`; the dead-Pod collector checks
those values, the opt-in annotation, exact capabilities, host-PID mode, observed
rollout, and Ready source-node agents before creating PodChaos. A digest alone
does not grant the capability. Leaving the flag unset preserves the ordinary
capless profile and fails the passive dead-Pod preflight before fault injection.

The GCS installer is fresh-namespace-only for this profile: immediately before
applying the runtime-agent DaemonSet, it requires the named DaemonSet to be
absent. If lookup fails or any DaemonSet already exists, installation stops;
it never adopts or reapplies an existing object whose capability fields may
include an unmanaged `SYS_PTRACE` entry.

Before applying the `RhizaCluster`, `install` runs a create-only GCS identity
preflight with the control-plane KSA and the actual workload KSA. It confirms the
workload's metadata-server GSA, a successful read of a writer-created data
canary, and HTTP 403 for control-object read, create, update, and delete. The
preflight rejects emulator, metadata-endpoint, proxy, and credential-file
overrides; its result is a wiring check, not a recovery qualification. Failed
exact-version cleanup is recorded privately in `RHIZA_OUT` as
`gcs-iam-cleanup-pending-<nonce>.*`; do not treat that run as clean or reuse it.
If a create response or UID-scoped Pod cleanup is uncertain, a private
`gcs-iam-pod-cleanup-pending-<nonce>.*` artifact records only the verified
namespace, Pod name, UID, and reason. Review that UID before any manual cleanup;
the launcher never falls back to name-only deletion.

Then use the launcher's two phases with one fresh `RHIZA_OUT` directory:

- `install` with the administrator kubeconfig. It applies the repository CRDs,
  generates a one-day self-signed `rhiza-fencer-tls` Secret and matching
  `rhiza-fencer-ca` ConfigMap, applies `votercount_gcs_chaos.yaml.in`, waits for
  the fencer, renders and applies the node-qualified runtime-agent profile
  using that fencer Deployment UID, and captures the operator-generated
  bootstrap admin credential as a mode-0600 `admin-token` file for the runtime
  phase.
- `run` with the scoped runtime kubeconfig. It runs
  `votercount_gcs_chaos.py`, which proves the admission shapes by server-side
  dry-run and runs the positive scenarios (intact-WAL restart, online
  replacement, generation recovery). It removes the admin-token file after the
  driver exits.

The privileged objects (CRDs, ServiceAccounts, Roles, RoleBindings,
Deployments, ClusterRole/ClusterRoleBinding and `RhizaCluster`) belong to the
administrator phase; the bootstrap runtime Role is intentionally not widened to
create them. The driver deletes the run-owned cluster-scoped objects during
cleanup and fails the run if any remain.

The positive set also gates on the bootstrap-owned `rhiza-inspector-deny-all`
NetworkPolicy. The driver creates the paired `rhiza-inspector` and
`rhiza-inspector-control` Pods just-in-time, pins both to one shared node,
releases them again, and fails the run on a failing or inconclusive probe. What
it records is bounded: a same-node paired probe in which the canonical deny-all
has the expected shape and UID, is the sole NetworkPolicy in the namespace at
both recorded boundaries (before and after probing), and in which the
inspector's Kubernetes API egress was denied while the derived control stayed
reachable before and after. The namespace quota therefore has to admit seven
Pods (three voters, operator, authority, and the two probe Pods) while the probe
runs.

This wiring does not by itself qualify independent GCS mutation, canonical
ObjectStorage recovery, NoPVC recovery, or dead-Pod return. Those claims still
require a run with the real identities, measured runtime enrollment, and
preserved recovery/read-barrier evidence.

What that does not claim: the deny-all is not asserted to be the exclusive
possible cause of the denial, since a node-local, label-specific or
Pod-IP-specific CNI or host dataplane rule could also explain it, and the two
policy inventories are boundary snapshots rather than a lock over the probe
interval. This is single-node qualification only: no physical-host,
multi-node-partition or performance claim follows from it, cluster-level or
node-local enforcement outside namespaced `NetworkPolicy` remains unobserved,
and the dead-Pod failure modes stay unqualified until Part 2 runs after the
production fencing executor lands.

The harness is split behind `--scenarios` (launcher: `RHIZA_SCENARIOS`):

- `positive` (default): bootstrap opt-in and adoption, `voterCount == replicas`,
  intact-WAL restart, online replacement and generation recovery. This is the set
  that supports the "cooperative recovery on real GCS" claim.
- `dead-pod` (narrow native Go scenario available, not part of this Python
  selector): `run-gcs-ha-dead-pod` kills one captured voter Pod, requires the
  exact authenticated GCS IndependentV2 `Voter` proof, verifies automatic
  single-voter membership replacement and ACK/read-barrier durability, then
  observes the natural StatefulSet ordinal return with a new Pod UID refused
  before readiness. A separately named learner is a valid replacement; this
  does not claim the old ordinal became a voter. The scenario has not been
  qualified against a live GCS/Chaos Mesh run yet, and does not complete the
  broader D1-D4 matrix or establish other dead-Pod failure modes.
- `all`: both sets.

Both images must be digest-pinned (`name@sha256:<digest>`); the launcher rejects a
tag-only reference. The `install` phase creates `rhiza-fencer-tls` and
`rhiza-fencer-ca` in the run namespace before the authority and voters start.

Local direct execution uses IPv4 loopback port forwards. `RHIZA_LOCAL_PORT_BASE`
is optional and defaults to `18080`; when set it must be a canonical decimal
integer from `1024` through `65432`. The launcher verifies that the four HTTP
ports `base..base+3` and their Raft ports `base+100..base+103` are free on
`127.0.0.1` before minting credentials. It rejects an occupied listener rather
than using a healthy response from an unrelated local process.

Always preserve evidence before cleanup. Run `rollback-local` for direct mode or
`rollback` for federation mode with the exact original variables and receipt
directory. Rollback verifies ownership and exact absence; a partial or ambiguous
cleanup is a failure requiring manual investigation, not permission to delete a
broader bucket, prefix, namespace, or account.

## Unattended Kubernetes backend qualification

`automatic-operator-chaos` runs `automatic_recovery.py` in a separate disposable
kind cluster. It creates one automatic `RhizaCluster` policy, then injects faults.
It never submits a `RhizaRecovery`, changes a recovery spec, provisions a learner,
or manually approves fencing. The Operator creates those actions from its journal.

The Kubernetes backend submits immutable `RhizaFence` requests. A test-only kind
executor establishes admission, runtime and isolated storage barriers before
reporting proof. Executor unavailability must leave recovery pending; restarting
the Operator must reuse the same request. Scenarios cover intact WAL restart,
Pod/WAL loss with online replacement, and QUIC partition of live old processes
followed by generation recovery, acknowledged-row preservation and new writes.

The executor has disposable-cluster administrator/runtime access. These privileges
are not added to the Operator, and the fixture is not a production shared-node
fencing service. Never run this harness against a shared cluster. A final `PASS`
event in the uploaded artifact is required to claim unattended qualification.

The automatic harness also gates learner startup in the test StatefulSet template
so UDP loss can be installed before membership admission. After the addition is
frozen it deletes that learner; the Operator must fence it, abort the addition,
provision a different identity, and complete recovery. The harness only releases
the injected startup gate; it does not issue an abort or create the replacement.

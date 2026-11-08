# Operator recovery chaos test

> **Historical qualification record.** The operator-chaos/MinIO recovery
> procedure below is retired, not an active CI job or supported recovery
> procedure. Current CI retains
> [legacy rejection tests](../../.github/workflows/ci.yml); the
> [managed bootstrap workflow](../../.github/workflows/ha-managed-bootstrap.yml)
> qualifies genesis and healthy-quorum repair, not whole-generation DR.
> For installation, use the [current managed guide](../../deploy/operator/README.md).

The former `operator-chaos` CI job built both binaries from the candidate revision
and ran them with MinIO in an ephemeral kind Kubernetes cluster. It applied the
repository CRD and namespace-scoped RBAC and drove the running Operator through
`RhizaRecovery` resources.

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

**Historical command; do not run as a current workflow.** The former job used
an isolated kind cluster named `rhiza-operator-chaos` with the test images
`rhiza-chaos:local` and `rhiza-operator-chaos:local`:

```sh
python3 -u e2e/chaos/operator_recovery.py
```

The former CI workflow contained the image and cluster setup commands. The harness
created namespace `rhiza-chaos` in the disposable cluster.

## GCS post-release qualification

**Historical procedure only.** This section records the former GCS qualification
contract; it is not a current deployment or recovery workflow.

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

**Historical commands; do not run as a current workflow.** The former
procedure reviewed the offline render with fresh `RHIZA_OUTPUT` and exact SHA
inputs:

```sh
sh e2e/chaos/bootstrap-gcs-postrelease.sh render-local > /tmp/rhiza-auth-review.yaml
sh e2e/chaos/run-gcs-postrelease.sh render
```

The historical direct execution used `apply-local`, `mint-local`, and
`run-local` after reviewing scope, receipts, digests, expiry, and cost cap;
federation used `apply` and `run`. The one-shot procedure prohibited reusing
an output or receipt directory, adopting colliding resources, or rerunning
after an unknown outcome.

## voterCount operator path (GCS + Chaos Mesh)

**Retired launcher; do not execute this sequence.**
`run-votercount-gcs-chaos.sh` now exits before any external action. The former
v0.19.5 voterCount path used `bootstrap-gcs-postrelease.sh apply-local` to
create the namespace, workload-identity bindings for `rhiza-gcs` and
`rhiza-operator`, quota, and admission policies. The historical launcher's
two phases shared one fresh `RHIZA_OUT` directory:

- `install` with the administrator kubeconfig. It applies the repository CRDs,
  generates a one-day self-signed `rhiza-fencer-tls` Secret and matching
  `rhiza-fencer-ca` ConfigMap, applies `votercount_gcs_chaos.yaml.in`, and
  captures the operator-generated bootstrap admin credential as a mode-0600
  `admin-token` file for the runtime phase.
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
- `dead-pod` (DEFERRED): D1-D4. Not executed by default; they run only after the
  production fencing executor lands (separate implementation project), so the
  dead-Pod claim is unqualified until then.
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

The historical cleanup preserved evidence before using `rollback-local` for
direct mode or `rollback` for federation mode with the exact original variables
and receipt directory. Rollback verified ownership and exact absence; a partial
or ambiguous cleanup required manual investigation, not deletion of a broader
bucket, prefix, namespace, or account.

## Unattended Kubernetes backend qualification

**Historical qualification only; not an active CI job.** The former
`automatic-operator-chaos` job ran `automatic_recovery.py` in a separate
disposable kind cluster. It created one automatic `RhizaCluster` policy, then
injected faults. It did not submit a `RhizaRecovery`, change a recovery spec,
provision a learner, or manually approve fencing. The former Operator drove
those actions from its journal.

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

# Operator recovery chaos test

The `operator-chaos` CI job builds both binaries from the candidate revision and
runs them with MinIO in an ephemeral kind Kubernetes cluster. It applies the
repository CRD and namespace-scoped RBAC and drives the running Operator through
`RhizaRecovery` resources.

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

Local-direct execution does not require GitHub credentials. Optional CI
federation additionally requires `RHIZA_GITHUB_REPOSITORY`, its numeric
repository and owner IDs, an exact workflow ref at the protected SHA, a protected
environment, and a fresh CI service-account ID. The bootstrap verifies the GCP
project ID/number and, in federation mode, the repository/owner IDs and workflow
file before creating cloud resources.

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

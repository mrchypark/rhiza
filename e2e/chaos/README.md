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

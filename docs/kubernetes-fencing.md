# Cooperative Kubernetes fencing

The default automatic-recovery backend is Kubernetes. `KubernetesFencer`
delivers immutable `RhizaFence` requests; **`rhiza-fencer serve` must also be
installed** to execute them. The `http` backend remains available explicitly
for external fencing adapters. This default needs no cloud-specific adapter,
host CRI socket, privileged DaemonSet, admission webhook, or node mutation.

## Supported deployment boundary

Enroll a **new generation before its first writer starts**. Every application
process must start through `rhiza-fencer supervise`, including every restart.
An already-running generation with unregistered or orphan writers cannot be
retroactively qualified by changing its Pod annotation. Do not treat absence
from the Kubernetes API as evidence that an old writer never existed.

The supported workload is a dedicated Linux container with the supervisor as
PID 1, a private PID namespace, one application container, dropped capabilities,
no privilege escalation, no host mounts, no init/ephemeral containers, and no
additional credential-bearing writers outside the enrolled workload. Node IDs
are stable Pod names. The application and its image are trusted; this is not
an isolation boundary against a compromised application sharing the supervisor's
service-account token. Administrators who replace images, edit protected
ledgers, inject processes, or bypass the wrapper can invalidate the contract.

The authority verifies the projected token's audience and Pod UID using
TokenReview, the current Pod's owner UID and source configuration, and the
single-container wrapper contract. Automatic learners additionally bind the
root cluster UID, source StatefulSet UID, operation, and immutable credential
Secret UID; they do not become unrelated, unregistered processes. Learners are
provisioned before their recovery child exists, so a missing child UID at that
stage is not fabricated or treated as an authority credential.

## Evidence and failure behavior

The authority CAS-registers a fresh run before the supervisor spawns its child.
The same ConfigMap CAS closes a voter slot or the entire source generation.
The child passes the local startup gate before `Node.Open` and binds its WAL
once before any writer starts. A concurrent fence either includes that run or
rejects its startup/bind. Every old incarnation remains in the ledger.

For a stop request, the engine closes admission, joins foreground, peer and
maintenance writers, and drains its lifetime storage-mutation tracker. Unknown
provider outcomes remain unknown even if an SDK retry later succeeds. A clean
engine receipt is persisted first. The supervisor then kills and waits for the
child and kills/reaps remaining processes in its private PID namespace before
recording separate process-exit evidence. Only matching durable evidence,
closed recreation admission, and the complete registered inventory permit a
`processesTerminated/recreationBlocked/storageQuiesced` proof.

This is a useful **reachable, cooperative** fencing path, not power fencing.
A hard crash, unreachable supervisor, unresolved old registration, unknown
storage mutation, missing historical registration, or unsupported Pod stays
pending. A supervisor restart with an empty local tracker does not prove its
previous run safe. Such incidents need independent stronger fencing evidence
from an optional external adapter or a carefully verified manual recovery;
never delete the ledger or restart the object store to manufacture a pass.

Voter fencing permanently closes that stable slot in the source generation but
does not stop surviving voters. A later generation fence preserves immutable
prior stop receipts and may reuse completed proof. A prior pending stop still
has to finish; escalation does not replace its evidence with a fresh tracker.

## Installation

Use the same versioned/digest-pinned images for server, supervisor, and authority.
Both repository Dockerfiles include `/usr/local/bin/rhiza-fencer`. Embedded
applications must include that binary in their own image and use the Go startup
integration; unwrapped or unsupported hosts cannot produce this proof.

1. Install the operator CRDs, including `deploy/operator/fence-crd.yaml`.
2. Provide a TLS Secret `rhiza-fencer-tls` (`tls.crt`, `tls.key`) whose certificate
   is valid for `rhiza-fencer` and its namespace-qualified Service name. Provide
   its issuing CA as `rhiza-fencer-ca`, key `ca.crt`. Use your normal certificate
   management; no private key belongs in Git.
3. Review and apply `deploy/operator/fencing/authority.yaml`. Its namespace Role
   can update fencing ledgers/status and read workload configuration. The
   additional ClusterRole can create TokenReviews only; it cannot modify nodes
   or Pods. Use namespace-specific ClusterRoleBinding names for multiple installs.
4. Apply the supervised StatefulSet patch to a **fresh** no-PVC generation. The
   file `deploy/operator/fencing/supervised-statefulset.patch.yaml` contains a
   strategic-merge patch and a separate application ServiceAccount document;
   split these documents in your manifest renderer. The application account has
   no Kubernetes RoleBinding. The token is audience-bound and rotates every ten
   minutes; the client rereads the token file for each request.
5. Set `RHIZA_AUTOMATIC_RECOVERY=true` on the operator. Leave
   `RHIZA_FENCER_BACKEND` empty (default) or use `kubernetes`. The external HTTPS
   adapter requires the explicit `http` backend and its existing URL/token/CA.

The supervisor injects `RHIZA_FENCE_SOCKET` and `RHIZA_FENCE_TICKET` into its child.
Neither is a substitute for Kubernetes authentication. Missing half of this
configuration fails startup. The application still needs its normal cluster,
member credentials, object-store configuration, and
`RHIZA_ENABLE_RECONFIGURATION=true` opt-in where online replacement is desired.

Do not remove fencing ConfigMaps or closed operations while their source
generation could still have authority. Ledgers deliberately fail closed at a
bounded size instead of evicting unresolved history. Ordinary graceful shutdown
without the exact terminal request is not a fencing receipt.

## Verification scope

Package tests cover identity, receipt ordering, CAS races, immutable evidence,
restart uncertainty, and voter-to-generation escalation. Linux Kubernetes
qualification must exercise the real supervisor/authority and physical storage
drain without the test-only MinIO-restart fencer. Legacy Kind-fencer test success
is not evidence for this backend. No GCS, Chaos Mesh, cloud cost, release, or
unreachable-node power-fencing qualification is implied by package tests.

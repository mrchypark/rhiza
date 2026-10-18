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

Exec probes and lifecycle process hooks are unsupported because they could
start work after the namespace was inspected. The container must retain normal
kernel procfs at `/proc`; the supervisor verifies its filesystem and PID-1
view before collecting termination evidence.

Keep the root filesystem read-only, but provide a writable `emptyDir` at `/tmp`
as the supervised patch does. LatticeDB checkpoint restore creates temporary
read-only graph materializations there; without this mount a recovered engine
cannot open its snapshot. This directory is scratch space, not persistent data
or fencing evidence.

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

If closure wins before WAL bind is authorized, the authority can record a
separate **never-bound exit** only after the supervisor waits for the child and
reaps its namespace. The closed CAS history proves this run never obtained
writer authorization. This is not a fabricated clean drain or invented WAL.
If a bind committed but its response was lost, its persisted WAL makes this
exception ineligible; normal drain or independent fencing evidence is required.

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
pending. A container restart within the same Pod UID may reserve a fresh run
while admission is open, but must bind its exact historical WAL before writers
start. A different Pod UID cannot bypass an unresolved prior voter. Restarting
does not prove an old run safe: every old record remains, and a later fence stays
pending without its evidence. Such fencing incidents need independent stronger evidence
from an optional external adapter or a carefully verified manual recovery;
never delete the ledger or restart the object store to manufacture a pass.

Voter fencing permanently closes that stable slot in the source generation but
does not stop surviving voters. A later generation fence preserves immutable
prior stop receipts and may reuse completed proof. A prior pending stop still
has to finish; escalation does not replace its evidence with a fresh tracker.

After a verified whole-generation fence, the operator scales the StatefulSet
to zero and cleans up supervised standalone learners from the exact fenced
incarnation inventory using UID-preconditioned Pod deletion. The operator's
namespaced Pod role therefore includes `delete`; it does not force-delete nodes
or treat a deletion response as fencing proof. All source inventory must still
disappear before the new generation can start.

### Pod deletion scope

`--delete-namespaces` (environment default `RHIZA_OPERATOR_DELETE_NAMESPACES`)
limits post-fence Pod deletion independently of the watched `--namespace`:

- Unset or empty: only the operator's actual namespace from its mounted service
  account namespace file. If `POD_NAMESPACE` is supplied, it must match that file.
  Missing or inconsistent self-namespace evidence stops startup; the watched
  namespace is never substituted as deletion authority.
- `team-a,team-b`: an explicit namespace allowlist. Whitespace is trimmed and
  duplicates are harmless. Empty items and invalid namespace names are errors.
- `*`: explicit all-namespace deletion eligibility; it must be the only value.

This is an upper bound, not a multi-namespace watcher or an RBAC grant. The
operator still watches one namespace, fencing remains bound to exact workloads,
and Kubernetes RBAC must independently permit the request. The default manifest
keeps a namespaced Role; neither CSV nor `*` creates broader Roles/Bindings.
Only named Pod deletion with an exact UID precondition is available; collection,
node, namespace, and arbitrary resource deletion are not supported. To operate
on a watched namespace different from the operator's own namespace, explicitly
allow that namespace and grant only the corresponding namespace RoleBinding.

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
   `RHIZA_FENCER_BACKEND` empty (default) or use `kubernetes`. For compatibility,
   an empty backend with existing HTTP URL/token/CA configuration still selects
   HTTP and rejects incomplete configuration. An explicit backend selection
   wins. New external HTTPS adapter installations should set `http` explicitly.

The supervisor injects `RHIZA_FENCE_SOCKET` and `RHIZA_FENCE_TICKET` into its child.
Neither is a substitute for Kubernetes authentication. Missing half of this
configuration fails startup. The application still needs its normal cluster,
member credentials, object-store configuration, and
`RHIZA_ENABLE_RECONFIGURATION=true` opt-in where online replacement is desired.

Do not remove fencing ConfigMaps or closed operations while their source
generation could still have authority. Ledgers deliberately fail closed at a
bounded size instead of evicting unresolved history. Ordinary graceful shutdown
without the exact terminal request is not a fencing receipt.

### Planned restarts and upgrades

A successful application `Close` or exit code zero without an operation-bound
terminal drain leaves unresolved fencing history; it is not retroactively a
clean receipt. Same-Pod container restarts can continue the intact WAL while
admission is open, but a different Pod UID cannot replace an unresolved voter.
This does not support arbitrary rolling image updates. Keep `OnDelete` and use
the explicit terminal-fencing/new-generation procedure for planned replacement.
Old unknown history still needs independent evidence for a later fence, even
if a newer run exits cleanly. Cloud adapters are optional stronger alternatives,
not evidence silently supplied by this cooperative default.

## Verification scope

### Learner cleanup evidence

The automatic control journal retains each learner's source generation, creation
operation, immutable credential UID, Pod UID and authenticated WAL identity until
the operator can prove that the incarnation can no longer return. Generation
requests freeze that retained inventory before fencing. A Pod created before the
journal CAS completes is strictly rediscovered and journaled in a separate
reconcile before escalation; conflicting ownership blocks recovery.

Cleanup follows a validated full-generation proof, never a Pod deletion as proof.
An empty WAL is not a wildcard: only an exact cooperative ledger snapshot whose
hash matches the proof, whose generation closure is complete, and whose every
run for that Pod is a verified never-bound exit permits this case. External HTTPS
proofs retain their existing bound-incarnation contract; they do not implicitly
authorize never-bound cleanup. Operator RBAC already includes ConfigMap `get`.
History is discarded as soon as the operator verifies the exact terminal proof,
the Pod UID is gone, and the incarnation's credential is deleted or superseded.
The controller retries every retained incarnation on later reconciles, including
entries from generations that are no longer active, so a temporary proof failure
does not permanently consume the journal. An incarnation missing any part of that
proof stays in the journal and remains fenced; a Pod deletion alone is never
proof. A completed external fence request with its exact proof is also retained as
a retirement receipt, so compaction does not require a cooperative ledger from
non-cooperative backends. Every retained run for the same Pod UID must be proven;
one clean run cannot hide an unknown run. Pruning does not mutate an already issued fence request. Pruned identities
are not retained as tombstones: a learner that reappears without a matching
journal entry is rejected by the existing unlisted-learner cleanup guard and
blocks recovery. Preexisting journals that discarded a retired learner's identity
cannot reconstruct that authority from Pod absence; they remain blocked and
require independently verified recovery. Learner history is capped at 64 entries,
and a record that cannot be compacted below the cap blocks with a precise remedy instead of
exceeding the control-record read limit.

Package tests cover identity, receipt ordering, CAS races, immutable evidence,
restart uncertainty, and voter-to-generation escalation. Linux Kubernetes
qualification must exercise the real supervisor/authority and physical storage
drain without the test-only MinIO-restart fencer. Legacy Kind-fencer test success
is not evidence for this backend. No GCS, Chaos Mesh, cloud cost, release, or
unreachable-node power-fencing qualification is implied by package tests.

# Rhiza recovery operator: v1alpha2 managed HA

## Responsibilities and deployment shape

The v1alpha2 managed route uses Deployment Pods, two init containers and an
`emptyDir`, not a PVC. It is an explicit reference-host integration, not a
turnkey adapter for an arbitrary application image or a library-only upgrade.
The image must provide `rhiza-ha-init`; the current managed Pod builder starts
its `serve-managed-sql` reference host as the main process.

| Component | Current responsibility |
| --- | --- |
| `rhiza-operator` | Validate the installed private runtime profile and matching Secret, create/read back the generation-zero Deployment, and commit genesis/bootstrap authority from actual gated Pod identities. Consensus certification does not require it after bootstrap, but external replica control does: the operator loop must remain running as the sole generated-Deployment `/scale` writer. Manual mode may run with it off. It is retained for separately authorized DR work and has no automatic DR executor. |
| Kubernetes Deployment/kubelet | Maintain the configured Pod count and restart containers. Manual mode changes `voterCount` and the Deployment together; external mode changes the `RhizaCluster` scale target and the operator writes Deployment `/scale`. A replacement Pod has a new identity; Pod loss is not a certified leave, and restart alone does not grant voting authority. |
| Reference managed host | Run the background replacement loop, discover gated candidates, admit one learner at a time through certified ADD-first repair, and serve authenticated private leave from a surviving Host. |
| Rhiza engine | Validate WAL identity, membership and certified history, catch up the learner, and certify/apply decisions. |

See the [operator entry point](../../cmd/rhiza-operator/main.go),
[initialization reconciler](../../pkg/operator/initializer_v1alpha2.go),
[Pod/init contract](../../pkg/operator/bootstrap_members_v1alpha2.go),
[reference-host replacement loop](../../cmd/rhiza-ha-init/serve.go), and
[Host replacement coordinator](../../pkg/managedsql/replacement.go).
Without the installed managed profile, the operator CLI polls but refuses new
genesis before creating a Deployment because it supplies no common-plan
provider. The library initializer can create a zero-replica candidate when
given an explicit provider; that is not the managed database startup path.
The base manifest still references `rhiza-config` and `rhiza-object-store`;
the managed overlay replaces those dependencies with the profile Secret.

## Installing the managed reference host

CR-driven managed deployment is implemented, but installing the inert base
alone is not the managed installation procedure. Use the public sibling
[managed overlay](../operator-managed/) after preparing its prerequisites:

1. Choose the namespace, a reachable shared object-store namespace, the explicit
   append-only application profile (9 or 10), peer networking and private admin
   authentication. Provision storage and supply credentials through your own
   authorized mechanism; do not copy CI emulator credentials into a deployment.
2. Prepare a private runtime profile with the chosen protocol/evidence identity,
   `before-ack` durability, reconfiguration enabled, peer port and `InitImage`.
   Use reviewed immutable image references. `InitImage` must contain
   `/usr/local/bin/rhiza-ha-init`; the operator Deployment image must provide
   `rhiza-operator`. The workload image must implement the reference host
   contract, not merely expose an application executable.
3. Install the profile as Secret `runtime`, key `runtime.json`, in that namespace.
   It is mounted at `/etc/rhiza/managed/runtime.json` and selected with
   `--managed-runtime-profile`. The installed file and the profile Secret read
   by the reconciler must agree exactly, including `ProfileSecretName`.
4. Render and review `kubectl kustomize deploy/operator-managed` with your
   namespace and operator image overrides, then apply that reviewed output with
   strict server-side validation. This installs the operator/host ServiceAccounts
   and RBAC; it does not supply your private runtime profile or storage.
   Keep the Namespace, RoleBindings, Secret and CR in the same namespace.
5. Create a status-free `RhizaCluster` from the
   [managed example](automatic-example.yaml), with `genesisIntent: Create`,
   matching application protocol and reference-host image. Do not pre-create
   its managed Deployment or write genesis/bootstrap authority into status.
6. Observe the operator-created Deployment and its owner UID, completed identity
   init containers, committed bootstrap plan and the exact cluster-labelled
   Pods becoming Ready. A successful CR create or operator rollout alone is
   not a ready database. Use the overlay guide's first-install checks and an
   authenticated linearizable query to verify current quorum.

The existing six-lane Kind qualification covers this managed bootstrap and
healthy-quorum repair path with isolated provider emulators. It does not grant
live-cloud parity, install credentials for consumers, or implement automatic DR.

The same CI driver has a separate installation-only scope selected by
`RHIZA_MANAGED_PUBLISHED_INSTALL_ONLY=true`. It uses the pinned published image
`ghcr.io/mrchypark/rhiza-operator@sha256:2f7752353aa8796a074cf8cf3d91102f206d30cc0628cfd5fff4fcd37e555f40`
for the operator, init containers and host, without building a CI application
image. It stops after a status-free CR produces an owned Deployment, three
Ready NoPVC Pods and committed SQL ACK/exact readback. Its distinct
`ha-managed-published-install/published-install-result.txt` artifact is not
evidence for Rust/C ABI, Pod replacement, C3/C5 or Chaos. Without this explicit
flag, the original six-lane qualification continues unchanged.

`rhiza-prepare-identity` binds the actual Pod UID to its identity handoff.
`rhiza-await-bootstrap-plan` waits for the committed bootstrap/repair preparation
before the main runtime opens. They share the data `emptyDir` with the main
container. Main-container restart retains that directory; Pod replacement loses
it and must follow authenticated bootstrap/adoption and catch-up.

The [cluster schema](cluster-crd.yaml) requires a voter count from 3 through 64.
In manual mode the deployment owner changes `voterCount` and scales the Deployment
together; in external mode it changes only the scale target and the operator
writes Deployment `/scale`. See [Replica control](#replica-control). Consensus does
not require a running operator after bootstrap, but external replica control does.
Each new Pod remains behind the identity gate until the Rhiza Host admits one learner.
Managed leave is one voter at a time and waits for the certified exact target and
live write/readback. Pod loss does not trigger leave. Removed IDs cannot
re-enroll; leave uses no fence and claims neither storage completion nor DR.
Retry an unknown result only with the same ID while the current repair record
exists. This is bounded membership management. The initializer does not enforce
`Recreate` or create/manage peer and client Services; those remain explicit
deployment configuration.

### Replica control

Choose the replica-control mode when the cluster is created. The presence of
`spec.replicas` is immutable, so it must be selected by the initial manifest.

Manual mode omits `spec.replicas`. `spec.voterCount` is the target, and the
deployment owner changes it and the Deployment together. This mode does not
consume the CR's scale subresource and is not autoscaling. The operator may run
with it off after bootstrap.

External mode sets `spec.replicas` equal to `voterCount` at creation. Both are
bounded to 3-64. After genesis, change only `replicas`; `voterCount` records the
initial composition and normally stays unchanged.

```yaml
apiVersion: rhiza.mrchypark.dev/v1alpha2
kind: RhizaCluster
metadata:
  name: sql
  namespace: rhiza
spec:
  logicalID: application-db
  genesisIntent: Create
  applicationProtocol: rhiza-managed-request-append-only/v1
  voterCount: 3
  replicas: 3
  automaticRecovery: "OFF"
  provisionalRetentionSeconds: 172800
  deletionPolicy: Orphan
  workload:
    podTemplate:
      spec:
        containers:
          - name: app
            image: ghcr.io/mrchypark/rhiza-operator:v0.23.2
```

In external mode, HPA, KEDA, or an operator uses the `RhizaCluster` scale
subresource. Keep the operator loop running so it can write approved steps to
Deployment `/scale`. Never target the generated Deployment.

```yaml
scaleTargetRef:
  apiVersion: rhiza.mrchypark.dev/v1alpha2
  kind: RhizaCluster
  name: sql
minReplicas: 3
maxReplicas: 10
```

The same proposal can be made directly:

```sh
kubectl scale rhizacluster/sql --replicas=5 -n rhiza
kubectl get rhizacluster/sql -n rhiza \
  -o jsonpath='{.status.replicas}{"\t"}{.status.appliedVoterCount}{"\n"}'
```

`status.replicas` is the observed Deployment replica count.
`status.appliedVoterCount` is the certified voter count and can lag while the
operator applies the target one step at a time.

The operator remains the sole writer of the Deployment's `/scale` resource. A
Host records a durable, unexpired approval tied to the active CR UID/generation
and Deployment UID/revision. Downscale additionally requires a fresh, one-use
proof after the Host rechecks quorum. Missing, stale, expired, or mismatched
proof fails closed. A changed proposal does not rewrite or strand an in-flight
step.

Targets below 3 are rejected. A physically forced one-Pod state is a disaster
state, not an autoscaled cluster: local reads may continue, while writes and
linearizable reads fail closed until the original WAL-backed voters and network
are restored or a separately authorized generation recovery establishes
certified state and a new quorum. Recreating or rescaling empty Pods is
insufficient. This path performs no automatic DR or physical fencing.

QuePaxa uses `floor(voterCount/2)+1` responses for a decision. Thus 4 and 5
voters both require 3 responses; 7 requires 4 and 10 requires 6. The leader fast
path means one epoch leader submits first, not that one voter decides alone.

The shipped [RBAC](rbac.yaml) grants more than this reconciliation path uses:
the operator Role includes Pod and Secret creation/deletion, Service creation,
and recovery-object permissions. The Host Role can read cluster resources
and update their status across the namespace by default. These grants do not
imply that a Service or DR controller runs. Use a trusted namespace and scope
the Host's cluster and status rules to known CR names where practical.

Normal images contain `rhiza-operator` and `rhiza-ha-init`. A separate fencer
Deployment or external lease service is not required for normal quorum repair.
The reference-host loop adds a fresh voter ID/WAL, verifies certified admission,
then removes the failed identity. Lost WALs never reuse voter IDs;
same-identity rejoin requires the original WAL and current certified membership.
HA object data is append-only, with no online delete GC.
Normal managed HA supports only the explicit policy-9 request and policy-10
Graph append-only profiles. Retired policy-7/8 profiles are not success lanes
or implicit defaults; their data must not be relabeled or silently migrated.
After startup and initial live admission, ordinary requests use the engine;
they do not reserve/finalize a Kubernetes CR for every application request.
Preserve pending state and UNKNOWN outcomes; use the exact-original recovery
contract in the [managed API guide](../../docs/managed-internal-request-api.md).

Without quorum, writes and linearizable reads fail while local reads remain
available on ready nodes. Restore the original quorum or stop for separately
authorized DR. The operator does not currently implement automatic generation
recovery. A Pod deletion or timeout is not proof of process termination,
recreation prevention or completion of outstanding storage work.

## Approved automatic-attempt policy, not implemented or qualified

The `automaticRecovery: OFF` schema default and the existence of recovery
request/status APIs do not mean that a whole-generation DR executor exists.
The current operator does not execute that workflow, even when a request object
or another schema policy value is accepted. The policy below is a design
boundary, not an installation switch.

Automatic DR attempts default to OFF. Explicit policy may allow preparation
after sustained actual ReadIndex/quorum failures, not just Pod readiness or a
failed probe. Activation requires evidence for the complete old runtime,
voter and sidecar scope: termination, recreation prevention and completion of
in-flight storage work. The verified source must cover every successful ACK,
and the replacement must form a new ready quorum before service resumes.
Unknown evidence or network partition leaves recovery Blocked for manual
action. Infrastructure fencing semantics remain necessary at this boundary;
an always-on fencer service is not mandatory for normal HA. This design adds
no new lease, epoch or live permissions. Preserve pending application state,
UNKNOWN outcomes and live anchor/Transition validation. The current runtime
does not yet implement or qualify this automatic-attempt policy.

The legacy `Controller.Run` recovery engine remains unavailable. The old alpha1
success-recovery CI and qualification-image publication paths are retired,
not passed or carried forward as alpha2 evidence. Their Go protocol regressions
and explicit no-side-effect rejection tests remain in CI. The current managed
contract, provider and kubelet workflows run on pull requests and main.
These examples are not a claim of complete disaster recovery qualification:
Chaos Mesh, isolated-process physical fencing and live-cloud parity require
separate evidence. Existing private HA code does not provide legacy API support.
Both HA kinds serve and store v1alpha2 only; there is no v1alpha1 conversion or
compatibility path. Existing object lifecycle is an installation prerequisite;
these files do not authorize deletion, adoption, or migration.

Submit resources with strict server-side field validation (for example,
`kubectl apply --validate=strict -f resource.yaml`); unknown fields may
otherwise be pruned before admission. Actual API-server admission validation
is required in CI; schema inspection alone is not that validation.

The pod template supports only `spec.containers` with a name and image. The
schema permits 1–64 entries, but the managed reference-host builder requires
exactly one. Schema acceptance is not proof that an arbitrary image implements
the reference host or a safe restore integration. Automatic recovery defaults to OFF,
provisional retention defaults to 48 hours, and deletion defaults to Orphan.
Neither deletion policy authorizes deletion of application storage evidence.

Recovery targets the exact same-namespace cluster name and UID. Read the live
authorized binding and fresh UID before creating a request: the example UID
and generation 0 are placeholders, not authority. Retry limit defaults to six
and is immutable in effective value; the optional positive deadline is also
immutable. TTL is mutable before or after completion, applies to Complete and
Failed request records only, and remains subject to finalizer and evidence
gates. Omitted TTL retains the record; it does not delete workloads or data.

The existing `no-pvc/` StatefulSet inputs and fence manifests are retained as
legacy references, not normal managed installation dependencies. Do not apply
them as a current alpha2 recovery procedure. Legacy alpha1 operational examples
have no conversion path, and `RHIZA_AUTOMATIC_RECOVERY=true` is rejected.
Manual DR still requires complete independent evidence and live activation
authority; removing normal-image fencing does not weaken that boundary.

## Phase-two status and queue components

The status adapter updates only `status.recoveryQueue` with exact cluster UID
and resourceVersion preconditions. Missing or mismatched authority refuses;
ambiguous writes need exact readback, not a blind retry. Authority, pending
writes and unrelated status fields are preserved.

The queue selects from a complete current request list by creationTimestamp,
using UID to break equal timestamps deterministically. An existing claim is
never overtaken or released by these components. A protected request finalizer
and fresh request/authority observations are required; unknown, deleted or
changed records block. A [finalizer-install primitive](../../pkg/operator/finalizer_v1alpha2.go)
exists for an exact UID and resourceVersion, but no running recovery controller
invokes it. Safe claim release and trusted retention are not implemented in
this path. Two-resource observations
are not atomic, and a claim is bookkeeping, not permission to perform recovery.
`Controller.Run` remains unavailable. CI seeds authority only as a test fixture;
that seed is not a production genesis or recovery procedure.

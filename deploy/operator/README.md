# Rhiza recovery operator: v1alpha2 managed HA

## Responsibilities and deployment shape

The v1alpha2 managed route uses Deployment Pods, two init containers and an
`emptyDir`, not a PVC. It is an explicit reference-host integration, not a
turnkey adapter for an arbitrary application image or a library-only upgrade.
The image must provide `rhiza-ha-init`; the current managed Pod builder starts
its `serve-managed-sql` reference host as the main process.

| Component | Current responsibility |
| --- | --- |
| `rhiza-operator` | Poll namespace-scoped clusters, validate the installed private runtime profile and matching Secret, create/read back the generation-zero Deployment, and commit genesis/bootstrap authority and the plan from actual gated Pod identities. |
| Kubernetes Deployment/kubelet | Maintain the configured Pod count and restart containers. A replacement Pod has a new identity; Kubernetes restart alone does not grant it voting authority. |
| Reference managed host | Run the background replacement loop, discover replacement candidates, and coordinate certified ADD-first repair under a healthy quorum. |
| Rhiza engine | Validate WAL identity, membership and certified history, catch up the learner, and certify/apply decisions. |

See the [operator entry point](../../cmd/rhiza-operator/main.go),
[initialization reconciler](../../pkg/operator/initializer_v1alpha2.go),
[Pod/init contract](../../pkg/operator/bootstrap_members_v1alpha2.go),
[reference-host replacement loop](../../cmd/rhiza-ha-init/serve.go), and
[Host replacement coordinator](../../pkg/managedsql/replacement.go).
Without the installed managed profile, the default initializer only creates
an inert candidate; it is not the managed database startup path.

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
   The image must contain `rhiza-operator` and `rhiza-ha-init`; use a reviewed
   immutable image reference. The workload image must implement the reference
   host contract, not merely expose an application executable.
3. Install the profile as Secret `runtime`, key `runtime.json`, in that namespace.
   It is mounted at `/etc/rhiza/managed/runtime.json` and selected with
   `--managed-runtime-profile`. The installed file and the profile Secret read
   by the reconciler must agree exactly, including `ProfileSecretName`.
4. Render and review `kubectl kustomize deploy/operator-managed` with your
   namespace and operator image overrides, then apply that reviewed output with
   strict server-side validation. This installs the operator/host ServiceAccounts
   and RBAC; it does not supply your private runtime profile or storage.
5. Create a status-free `RhizaCluster` from the
   [managed example](automatic-example.yaml), with `genesisIntent: Create`,
   matching application protocol and reference-host image. Do not pre-create
   its managed Deployment or write genesis/bootstrap authority into status.
6. Observe the operator-created Deployment and its owner UID, completed identity
   init containers, committed bootstrap plan and the exact cluster-labelled
   Pods becoming Ready. A successful CR create alone is not a ready database.

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

The [cluster schema](cluster-crd.yaml) requires `voterCount` from 3 through 64
and makes it immutable. This is a fixed target, not autoscaling. The initializer
does not enforce `Recreate` or create/manage peer and client Services; rollout
policy, network reachability, service exposure and application integration need
explicit deployment configuration. `Recreate` is not a termination or fencing
proof for forced deletion or partitions.

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

The pod template supports only `spec.containers` with a name and image. Its
accepted shape is not proof that an arbitrary image implements the reference
host or a safe restore integration. Automatic recovery defaults to OFF,
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
changed records block. Finalizer installation, safe claim release and the
trusted retention protocol are not implemented here. Two-resource observations
are not atomic, and a claim is bookkeeping, not permission to perform recovery.
`Controller.Run` remains unavailable. CI seeds authority only as a test fixture;
that seed is not a production genesis or recovery procedure.

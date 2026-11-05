# Rhiza recovery operator: v1alpha2 managed HA

The v1alpha2 managed runtime uses Deployment Pods, two init containers and no PVC.
The managed initialization route is selected with a private runtime profile;
CI verifies actual bootstrap, Pod replacement, main restart and ACK readback.
Normal images contain `rhiza-operator` and `rhiza-ha-init`; a separate fencer
Deployment or external lease service is not required. With a healthy quorum,
managed repair uses certified ADD-first admission of a fresh voter ID/WAL,
followed by removal of the failed identity. Lost WALs never reuse voter IDs;
same-identity rejoin requires the original WAL and current certified membership.
HA object data is append-only, with no online delete GC.
Normal managed HA supports only the explicit policy-9 request and policy-10
Graph append-only profiles. Retired policy-7/8 profiles are not success lanes
or implicit defaults; their data must not be relabeled or silently migrated.

Without quorum, writes and linearizable reads fail while local reads remain
available on ready nodes. Restore the original quorum or stop for separately
authorized DR. The operator does not currently implement automatic generation
recovery. A Pod deletion or timeout is not proof of process termination,
recreation prevention or completion of outstanding storage work.

## Approved automatic-attempt policy, not implemented or qualified

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
restore integration still requires validation; an accepted template is not
proof of safety. Automatic recovery defaults to OFF,
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

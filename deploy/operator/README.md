# Rhiza recovery operator: v1alpha2 managed HA

The v1alpha2 managed runtime uses Deployment Pods, two init containers and no PVC.
The managed initialization route is selected with a private runtime profile;
CI verifies actual bootstrap, Pod replacement, main restart and ACK readback.
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

The existing `no-pvc/` inputs and fence manifests are retained separately;
they do not implement this proposed contract or establish a safe recovery.

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

# Rhiza recovery operator: phase 1 contract

The v1alpha2 manifests describe a proposed Deployment and NoPVC HA contract.
The public operator Run path is blocked until the safe engine is implemented;
these examples are illustrative, not deployment instructions or a release.
Existing private HA code and its internal tests do not provide legacy API support.
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

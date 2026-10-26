# Logical-writer NoPVC deployment profile

This directory is a replacement profile for the runtime-bound resources in
`deploy/operator/fencing/authority.yaml` and
`deploy/operator/fencing/supervised-statefulset.patch.yaml`. Do not apply both
profiles together. The authority command uses the source-supported
`--authority-profile=logical-writer-v1` and `--source-writer-use-adc` flags.

Before rendering, pin both images by immutable registry digest and provision:

- the `rhiza-fencer-tls` Secret and `rhiza-fencer-ca` ConfigMap;
- `rhiza-config` with public cluster/reconfiguration settings and
  `rhiza-object-store` with public GCS coordinates and before-ack durability;
- `rhiza-peer-credentials` for the main container only; and
- distinct workload-identity bindings: `rhiza-voter-reader` must have read-only
  access to the selected archive, while `rhiza-fencer` must have the canonical
  authority/source mutation permissions required by the logical profile.

No static writer key or writer credential Secret is included. The authority
uses its own ADC identity; restore-init and the main container use only the
reader identity. The authority object-store policy must be scoped to the
selected cluster's canonical state and source prefix; Kubernetes RBAC alone
does not constrain those cloud permissions.

The init container runs the same pinned `rhiza` image with `restore-init`, has
read-only archive access, and writes only to the separate staging `emptyDir`.
The main container mounts that stage read-only and owns a fresh `DataDir`
`emptyDir`. Init does not receive peer credentials or the authority token/CA.
During recovery, the controller publishes an immutable public-only ConfigMap
named `<recovery-secret-name>-membership` from the validated membership in its
existing target Secret, then references it from main and restore-init. The
target Secret remains unchanged; only its `members` value is projected, never
its admin or peer-token values. The init container receives the generated
ConfigMap by a direct key reference and no Secret reference. Do not create a
manually named membership ConfigMap. Apply this template as part of the
controller-coordinated target replacement; do not manually delete/recreate a
source Pod before the controller has published the target projection and
rewritten the init environment.

This is a template for a workload with an already configured, certified HA
archive and trusted membership. It does not select or create an empty genesis
root. Fresh-cluster bootstrap remains unavailable until the operator can
authoritatively select and persist a certified genesis/archive source. Do not
set bootstrap true or interpret a missing archive as an empty cluster. The
profile is not Kubernetes/GCS qualification evidence; runtime replacement,
quorum-loss recovery, and exact-digest provider qualification remain separate
acceptance gates.

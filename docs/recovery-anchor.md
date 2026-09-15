# Recovery Anchor

The Kubernetes Operator orchestrates recovery (fencing, StatefulSet control,
fork, seal). The recovery-anchor service provides two capabilities: anchor
CAS and verification via `recoveryanchor.Coordinator`. The embedded
application does NOT write its own recovery orchestrator.

## Activate protocol

Two-phase CAS via `Coordinator.Activate`:

1. **Transition reserve** — freezes current source binding, generation, and
evidence into `Record.Transition`. CAS writes with outer `PendingWrite=false`.
2. **Commit** — calls Verifier which returns `(targetBinding, proofBytes)`.
Proof must match frozen evidence. Commits with `Binding=targetBinding`,
`Generation=baseGeneration+1`, clears `Transition`, records `Receipt`.

`Receipt` contains `RequestHash`, `Target` binding, and `Generation`.
Idempotent: already committed returns existing `Receipt`.

## Verify protocol

`Coordinator.Verify(ctx, req, receipt)` live-checks:

- Record `Version==1`, `EvidenceFormat` matches
- `committed()` helper confirms request is committed
- `receipt == *record.LastTransition` (full struct comparison)

Does NOT restore checkpoints or compare current evidence. Normal writes
advance evidence via `UpdateApplication`; Verify is receipt/binding/generation
check only.

Same-generation startup MUST compare current DB state against current
external anchor evidence.

## App helpers

`CheckBinding(record, expected, expectedGeneration)` — validates record
against expected binding/generation. Rejects `PendingWrite` and active
`Transition`. Non-atomic; use within application writeprotocol.

`UpdateApplication(ctx, backend, id, version, binding, generation, evidence,
pendingWrite)` — updates app fields (evidence, pending flag) while
preserving recovery fields. Uses string resource version for CAS.
Record must have no active `Transition`.

## Evidence model

Stored evidence is the application schema proof — canonical
`json.Marshal(struct{Epoch int64; Token string}{...})` from the materializer
callback. Static epoch proof, NOT full DB rollback protection.

## Object store GenerationAnchor vs external Record

Object store `GenerationAnchor` (recovery/anchor.json) is the immutable
recovery artifact used by `VerifyGenerationAnchor` and
`MaterializeGeneration`. External `Record` (ConfigMap via Backend)
supplements it with binding, evidence, and transition state.
Missing external anchor never initializes from recovered data.

## Ternal migration

Existing Ternal guard/backend must adapt to the common Record format.
Migration preserves these fields from the source tree:

- `Format` — validate the legacy format and retain it in a versioned, application-specific migration envelope; Record.EvidenceFormat identifies the adapter's new evidence schema.
- `ClusterID` — mapped to `Binding.ClusterID`
- `StorageID` — preserve the stored identity only after verifying it equals the trusted storage recipe (`TrustAnchorStorageID`); mismatch blocks migration.
- `Epoch` (int64) — stored in `Record.Evidence` via canonical JSON
- `Token` (string) — stored in `Record.Evidence` via canonical JSON
- `PendingEpoch` — preserved in application-specific evidence fields
- `PendingID` — preserved in application-specific evidence fields
- `Bootstrap` — preserved in application-specific evidence fields

The adapter writes the common Record to ConfigMap while retaining all
original source fields. Normal write protocol continues through the adapter;
no promise of automatic compatibility with the old flat ConfigMap format.
No reset epoch or bootstrap during migration.

Binding recipe: `sha256(JSON([provider, endpoint, bucket, prefix, cluster]))`.
App-specific; not universal.

## Configuration

### Environment variables

| Variable | Description |
|----------|-------------|
| `RHIZA_NAMESPACE` | Kubernetes namespace (default: `default`) |
| `ANCHOR_TOKEN_FILE` | Path to bearer token file (required, empty denies all) |
| `ANCHOR_TLS_CERT` | TLS certificate file path (required) |
| `ANCHOR_TLS_KEY` | TLS key file path (required) |
| `RHIZA_OBJSTORE_PROVIDER` | `s3`, `gcs`, `azure`, or `filesystem` |
| `RHIZA_OBJSTORE_ENDPOINT` | Object store endpoint |
| `RHIZA_OBJSTORE_BUCKET` | Object store bucket |
| `RHIZA_OBJSTORE_PREFIX` | Object store prefix |

### AWS credentials (for S3 / MinIO)

`objstore.LoadConfig` uses the AWS SDK credential chain. For MinIO or
non-IAM environments, set:

- `AWS_ACCESS_KEY_ID`
- `AWS_SECRET_ACCESS_KEY`
- `AWS_REGION`

Do not use `RHIZA_*` keys for object store credentials.

### Operator environment

The Kubernetes Operator Deployment requires additional environment variables
to connect to the recovery-anchor service. See [embedded-operator.md](embedded-operator.md#6-recovery-anchor-연동) for the full configuration.

| Variable | Description |
|----------|-------------|
| `RHIZA_RECOVERY_ANCHOR_URL` | HTTPS URL of the recovery-anchor service (e.g. `https://anchor.svc:9191`) |
| `RHIZA_RECOVERY_ANCHOR_TOKEN_FILE` | Path to the bearer token file for the anchor service |
| `RHIZA_RECOVERY_ANCHOR_CA_FILE` | Path to the TLS CA certificate file for the anchor service |

Workload Pods must also set:

| Variable | Description |
|----------|-------------|
| `RHIZA_RECOVERY_ANCHOR_ID` | Anchor ConfigMap identifier (e.g. `myapp-production`) |
| `RHIZA_NAMESPACE` | Kubernetes namespace of the anchor ConfigMap |

The CRD fields `spec.anchorID` on `RhizaCluster` and `RhizaRecovery` link a
recovery operation to a specific anchor.


### ServiceAccount and RBAC

The anchor service requires a scoped ServiceAccount with permission to
get and update (not create/delete) a preexisting ConfigMap named
`rhiza-anchor-<id>`. Do not alter global RBAC. Example Role:

```yaml
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: anchor-configmap-access
  namespace: YOUR_NAMESPACE
rules:
  - apiGroups: [""]
    resources: ["configmaps"]
    resourceNames: ["rhiza-anchor-YOUR_ID"]
    verbs: ["get", "update"]
```

## TLS

Required. `ANCHOR_TLS_CERT` and `ANCHOR_TLS_KEY` must be set.
Empty or missing values are errors.

## Token auth

`ANCHOR_TOKEN_FILE` reads the bearer token from a file. **Empty token
denies ALL requests** — it does NOT disable auth.

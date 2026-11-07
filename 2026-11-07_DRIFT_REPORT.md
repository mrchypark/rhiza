# Managed HA installation drift report

Baseline: `ffd566387995f9d60a33402cdecb62fd293ddaee`. The discrepancies below were addressed with documentation changes only.

## docs/managed-graph-ha.md

- Baseline line 90 used `before_ack`, but the [configuration constant](internal/types/config.go#L27) and [managed Host check](pkg/managedsql/host.go#L88) require `before-ack`. Corrected the literal.

## deploy/operator/README.md

- Baseline lines 36–40 implied that `InitImage` requires both binaries. The [init container command](pkg/operator/bootstrap_members_v1alpha2.go#L89) uses `rhiza-ha-init`, while the [operator image entry point](Dockerfile.operator#L24) runs `rhiza-operator`. Separated the image requirements in the guide.

## docs/operator-go-contracts-2026-10-30.md

- Baseline line 1 called the appendix current, although it describes a [commit-scoped implementation](docs/operator-go-reverse-spec-2026-10-30.md#L3) and [current `Controller.Run`](pkg/operator/controller.go#L73) rejects legacy recovery. Labeled it as a historical snapshot, linked the current installation guide, and preserved the snapshot body.

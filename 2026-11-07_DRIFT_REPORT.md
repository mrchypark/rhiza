# Managed HA installation drift report

Baseline: `ffd566387995f9d60a33402cdecb62fd293ddaee`. The discrepancies below were addressed with documentation changes only.

## docs/managed-graph-ha.md

- Baseline line 90 used `before_ack`, but the [configuration constant](internal/types/config.go#L27) and [managed Host check](pkg/managedsql/host.go#L88) require `before-ack`. Corrected the literal.

## deploy/operator/README.md

- Baseline lines 36–40 implied that `InitImage` requires both binaries. The [init container command](pkg/operator/bootstrap_members_v1alpha2.go#L89) uses `rhiza-ha-init`, while the [operator image entry point](Dockerfile.operator#L24) runs `rhiza-operator`. Separated the image requirements in the guide.

## docs/operator-go-contracts-2026-10-30.md

- Baseline line 1 called the appendix current, although it describes a [commit-scoped implementation](docs/operator-go-reverse-spec-2026-10-30.md#L3) and [current `Controller.Run`](pkg/operator/controller.go#L73) rejects legacy recovery. Labeled it as a historical snapshot, linked the current installation guide, and preserved the snapshot body.

# Managed HA, operator and published-restore follow-up

Baseline: `0c0d3b9022cec0b83be63a87c4c69d30f2c2e9d6`. Scope: current managed HA deployment, request and recovery contracts, plus their qualification instructions. Dated reverse specs, releases, and explicitly unimplemented designs are not current feature promises.

## README.md

- Raw mutation resubmission and durability options were not clearly scoped away from managed requests. The managed Host requires `before-ack` and exact original recovery without resubmission.
- A tip observed before retry is not an atomic retention guarantee for a subsequent submission. The raw Go outcome procedure points to `ResolveExecute` with the exact original.
- The S3 replica configuration omitted the endpoint required by the SDK adapter.

## deploy/operator/README.md

- The profileless executable refuses genesis before Deployment creation. A zero-replica candidate belongs to the library path with an explicit plan provider.
- The CRD's broad container list is not the executable contract: the managed builder requires exactly one container.
- The finalizer primitive exists, but the active initializer does not run the recovery lifecycle that would use it.

## deploy/operator-managed/README.md

- The S3 profile example had a URL scheme where the SDK expects a host and optional port. TLS is selected separately.
- Operator rollout and local Pod readiness are not evidence of a live write quorum. The overlay's runtime behavior also does not describe the full granted namespace RBAC.

## docs/managed-internal-request-api.md

- The configured durability string is `before-ack`, not `before_ack`.

## docs/managed-graph-ha.md

- Certified fresh genesis can start without a checkpoint. The verified checkpoint requirement applies to Graph restoration, not every NoPVC startup.

## docs/quorum-membership.md

- The durability literal was incorrect, and the described Rust administrative methods belong to `Db`, not `ManagedDb`.

## sdk/rust/README.md

- The managed durability literal was incorrect. UNKNOWN handling requires the original operation kind and raw request ID for `recover_request`; the unqualified `recover_pending()` path is not a general exact recovery selector.

## docs/managed-async-recovery.md

- Policy 10 needs a published checkpoint locator and a verified SQLite/native Graph checkpoint pair. The excluded fixture operation was not merely present in the WAL: it was also certified and applied locally, but unpublished.

## docs/recovery.md

- Raw Async behavior and the managed Async proposal were not distinguished clearly. The implemented managed path rejects Async, and offline restoration does not grant write authority.
- The activation discussion conflated fencing evidence with publication coverage. The BeforeAck contract and proposed Async tail-loss boundary differ; neither is a current managed activation path.

## docs/recovery-anchor.md

- The coordinator verifies callback evidence and the anchor CAS; it does not independently prove process fencing, select a complete source cut, or wire managed generation activation.

## docs/automatic-recovery-design.md

- The approved automatic-attempt design and the later Async loss proposal have different coverage requirements. Neither implements whole-generation activation.

## deploy/operator/sample-rhizarecovery.yaml

- The example's application instruction implied an available executor. It is a schema illustration; submission does not run restore or activation.

## cmd/rhiza-ffi/README.md

- `recover_pending` is limited to an unambiguous local hint; the exact FFI lookup takes the original kind and base64 request ID via `recover_pending_detailed`.

## docs/azure-supervised-startup.md

- The legacy supervised constructor does not probe container existence. Managed HA also disables auto-creation, independently of that legacy supervisor.

## e2e/chaos/README.md

- The legacy MinIO/Kind recovery executor is not the current managed qualification route. Marked its procedures historical and linked the supported managed harness.

## e2e/fencing/README.md

- Legacy fencing qualification and image contents are not features of the current managed operator install. Labeled the evidence as historical.

## docs/ha-managed-cluster-chaos.md

- The documented manual route omitted candidate and direct-host variants and a second Pod replacement before the same-Pod restart. It did not distinguish reference-image SQL evidence, Rust-wrapper evidence, and Kind CRI proof.
- The shared overlay renderer needed a pinned `yq` binary and introduced private JSONL output. Manual CI lacked renderer setup and both manual drivers omitted that file from cleanup.
- The direct-host driver expected retired overlay resource names and lacked created-UID tracking for the current overlay. Its previous proof contract did not account for the second replacement or the distinct Kubernetes API vs. Kind CRI evidence.
- Release mode executes the selected tag's own harness. Incompatible historical tags, including v0.23.2, are rejected before cluster execution; the new harness is not evidence for an old tag. Scope: local shell oracles and workflow lint passed; live Kubernetes RBAC, deletion preconditions and Chaos injection were not rerun.

## docs/embedded-operator.md

No drift detected.

## docs/embedded-lifecycle.md

No drift detected in the managed HA scope.

## docs/embedded-object-storage.md

No drift detected in the managed HA scope.

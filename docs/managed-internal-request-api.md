# Managed internal request API

Status: this documents the explicit append-only profiles released in v0.23.1.
Release availability is not a guarantee for arbitrary deployments, cloud
partitions, or whole-cluster disaster recovery; the boundaries below still apply.

## Supported profile boundary

| Runtime | Durable policy | HA support |
|---|---:|---|
| Local | Local | Existing Local contract; unchanged |
| Legacy managed SQL | 7 | Retired for managed HA execution; historical read-only inspection only |
| Managed SQL/KV | 8 | Retired for managed HA execution; historical read-only inspection only |
| Managed internal SQL/KV | 9 | Explicit `rhiza-managed-request-append-only/v1` only |
| Managed Graph | 10 | See [managed Graph HA](managed-graph-ha.md); explicit append-only identity only |
| Retired request-9 prototype | 9 | `rhiza-managed-request/v1` is not a supported HA startup profile |
| Retired Graph-10 prototype | 10 | `rhiza-managed-request-graph/v1` is not a supported HA startup profile |

The numeric policy is not provenance. HA startup requires the exact protocol
and evidence identity, policy, generation, bootstrap-plan/member binding and
registered WAL identity to agree. The final append-only profiles currently
support generation 0 only. A missing, retired, unknown, or mismatched identity
must fail before opening an application writer/native runtime, publishing a
checkpoint, submitting application intent, or uploading a new object.

Selecting the final profile does not relabel, migrate, or reinterpret an old
WAL. Retain original bytes, anchors, and uncertain outcomes; use read-only
inspection for diagnosis. Do not silently fall back to a different profile.
Managed HA execution profiles 7/8, including their prior startup defaults, are
retired. Historical records may remain readable for diagnosis, but do not
authorize startup, application execution, or activation. Use an explicit 9/10
append-only profile for a new cluster; no automatic old-WAL migration is
provided. The separate Local contract remains unchanged.

## Request and recovery path

The operator initializes the managed Deployment and bootstrap plan. Kubernetes
maintains its Pods; the reference managed host discovers replacement candidates
and coordinates healthy-quorum repair through the Host membership APIs. The
engine verifies the certified membership, WAL identity, and replay prefix.
These are separate lifecycle responsibilities, not automatic whole-cluster DR.
After managed startup and initial live admission,
ordinary SQL/KV calls use the engine-owned Node/materializer path. They do not
create one Kubernetes CR reservation/finalization sequence per request or a
second application intent log. This is a control-path boundary, not a latency
or Kubernetes-request-count claim.

The existing Go `managedsql.Host` surface remains the entry point:

- SQL: `Execute`, `Query`.
- KV: `KVMutate`, `KVGet`.
- Status: `RequestStatus` is a local observation, not proof of remote object
  durability.
- Exact recovery: `RecoverRequest(ctx, network.RequestStatusRequest{Kind,
  RequestID})` resolves the engine-retained original. It submits no replacement
  write and does not accept caller payload as the original.
- `RecoverPendingDetailed` remains a bounded compatibility facade. If its
  per-handle hint is missing or ambiguous, use the exact selector.

Rust `rhizadb::ManagedDb` provides the same SQL/KV request and recovery
operations. `recover_request(MutationKind, request_id)` crosses the existing
`recover_pending_detailed` C ABI operation with an exact selector; the Go Host
remains the engine owner. Existing C symbols and the general error/response
contract are unchanged.

`commit_unknown` means the request may have committed. Keep the original kind,
raw request ID, intent, and any known slot. Resolve that original through
`RecoverRequest`/`recover_request`; do not make a new ID, change the payload, or
resubmit as recovery. The resolver checks the canonical retained original and
configured durability boundary. Missing/expired originals, uncertainty, and
conflicting intent remain unresolved or rejected; absence is not proof of no
effect. Deduplication and the canonical first receipt remain authoritative;
a duplicate submission may have a later slot.

The zero-proposal guarantee applies to recovery itself. It does not convert a
prior UNKNOWN response into success or permit recovery to clear runtime poison.
Validation, admission, and intent-conflict failures remain distinct from
`commit_unknown`.

## Qualification

The release includes engine-owned recovery and the exact-original receipt path.
Qualification must distinguish Go normal/race checks, actual provider-SDK
checks, native Rust/C ABI execution, and scoped Pod/Chaos Mesh experiments.
None establishes production-cloud parity or recovery from every partition.
The normal profiles require generation 0, append-only object data, and
`before-ack` object-store durability. No online HA delete GC or automatic
old-profile migration is implied. See [managed HA](managed-graph-ha.md) for
NoPVC restoration and deployment boundaries.

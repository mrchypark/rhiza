# Managed SQL/KV/Graph HA

Status: the explicit append-only profiles below are released in v0.23.1.
This is a bounded runtime contract, not a promise of turnkey integration for
arbitrary application images or recovery from every infrastructure failure.

## Profile support

| Profile | Policy | HA startup contract |
|---|---:|---|
| Local | Local | Existing Local contract; unchanged |
| Legacy managed SQL | 7 | Retired for managed HA execution; historical read-only inspection only |
| Managed SQL/KV | 8 | Retired for managed HA execution; historical read-only inspection only |
| Retired request prototype `rhiza-managed-request/v1` | 9 | Reject for HA startup; preserve its WAL and bytes |
| Retired Graph prototype `rhiza-managed-request-graph/v1` | 10 | Reject for HA startup; preserve its WAL and bytes |
| Final managed request `rhiza-managed-request-append-only/v1` | 9 | Explicit append-only identity required |
| Final Graph `rhiza-managed-request-graph-append-only/v1` | 10 | Explicit append-only identity required |

Policy 9 or 10 alone does not select a supported profile. The exact protocol,
evidence format, policy, generation, bootstrap-plan and member-config binding,
and registered WAL identity must match the persisted/runtime binding. These
profiles are restricted to generation 0; generation changes and membership
maintenance remain separately authorized control paths. A retired, unknown,
missing, or mismatched profile must fail before application writers/native
state, checkpoint publication, new application intent, or object upload begins.

No automatic old-WAL migration or relabeling is supported. Preserve original
bytes, source anchors, and UNKNOWN outcomes. Do not fall back to another
protocol because a file happens to carry policy 9 or 10. Low-level read-only
inspection may be used to diagnose old data, but does not enable HA startup.
Managed HA execution profiles 7/8, including their prior startup defaults, are
retired. Historical records may remain readable for diagnosis, but do not
authorize startup, application execution, or activation. New clusters must
select an explicit 9/10 append-only profile. The separate Local contract
remains unchanged.

## Go, Rust, and C ABI

The existing Go `managedsql.Host` owns execution. Its Graph surface includes:

- Mutations: `GraphExecute`, `SetGraphStreamOffset`, `TrimGraphStream`.
- Reads: `GraphQuery`, `GraphReachable`, `GraphChanges`, `GraphStreamRead`,
  `GraphStreamOffset`.
- Status and exact recovery: `RequestStatus` and `RecoverRequest` with
  `Kind: "graph"` and the original request ID.

Rust `rhizadb::ManagedDb` exposes the corresponding `graph_execute`,
`graph_query`, `graph_reachable`, stream methods, and
`recover_request(MutationKind::Graph, original_id)`. Graph mutations cross the
existing C ABI `graph_execute` operation as the typed `frame_b64` execution
frame; the ordinary JSON Graph mutation DTO is not a second original. The
Rust API keeps request IDs and typed string/map-key bytes lossless, returns the
actual mutation receipt, and preserves `commit_unknown` context through
`ManagedMutationError`. Existing C symbols and response/error types are not
replaced.

Use the existing typed Graph validators and resource limits. The managed C ABI
execution frame decoder currently allows at most 256 KiB; the ordinary Graph
projection and engine validation retain their own existing constraints. This
document adds no alternative limit or larger-payload promise.

## Normal requests and control path

```text
operator initialization / verified profile / bootstrap plan
                      │
                      ▼
     exact profile + generation + WAL binding + initial admission
                      │
                      ▼
     normal Go Host / Rust ManagedDb request → engine-owned Node

separate control path: membership, repair, certified checkpoint/archive,
                       object durability, startup recovery
```

After initial admission, normal reads, writes, status, and exact recovery use
the engine runtime; they do not create a per-request Kubernetes CR
reservation/finalization or a second application-intent log. This does not
bypass live lifecycle authority at startup, repair, policy, or membership boundaries,
and is not a request-latency or Kube-request-count claim.

An uncertain mutation may have committed. Keep its original kind, raw ID,
payload, and known slot; use exact recovery to resolve the canonical retained
original. Do not resubmit it, generate a new ID, or treat missing evidence as
proof of no effect. Recovery itself adds zero application proposals and does
not refresh retention or clear poison. A duplicate submission may be assigned
a later slot than the canonical first receipt.

The public managed Host requires `before-ack` object-store durability. An
asynchronous success ACK beyond the remote archive head is outside this
contract; a certified-only or uncertain result must not be reported as a
durable ACK.

## NoPVC and recovery boundary

The operator renders the reference managed host, identity/bootstrap init
containers, and an `emptyDir` data volume. Kubernetes maintains Deployment
replicas and restarts containers. The reference host's replacement loop
discovers candidates and coordinates ADD-first healthy-quorum repair; the
engine checks certified membership and restores/replays verified history.
This does not require a separate fencer for normal quorum membership changes.
The current initializer does not create peer/client Services or set a
Deployment `Recreate` strategy, and `voterCount` updates are unsupported.
Applications outside the reference host must integrate the corresponding
startup and repair contracts; the operator is not a generic application adapter.

An authenticated first bootstrap may create fresh SQLite/native Graph state
without a checkpoint. It must be an original bootstrap member with no existing
registered WAL or application state, no compaction floor or producer locator,
and a verified genesis prefix. For recovery, startup reconstructs the verified
SQLite/native Graph pair from the selected immutable checkpoint and replays the
contiguous certified suffix through the captured certified ACK tip of the
registered local WAL. Checkpoint
selection requires the exact QCKP, actual archive certificate/history, full
object hashes, and pair validation; a CURRENT locator is a hint, not authority.
Missing required roots, identity/prefix drift, incomplete installation, poison, or
ambiguous evidence fail closed rather than creating a new baseline.

Object data is append-only. Online delete GC is not part of this profile. Pod
API deletion is not proof that an isolated old process ended or that outstanding
object-store work completed. This release does not qualify whole-cluster
disaster recovery or fencing every arbitrary network partition; those remain
explicit gaps.

## Qualification status

Go normal/race checks, actual provider-SDK checks, native Rust/C ABI execution,
and scoped Pod/Chaos Mesh qualification establish different evidence. A pass
in one scope is not a substitute for another; emulator success is not
production-cloud parity. Automatic generation DR remains unimplemented, with
automatic-attempt policy defaulting to OFF. Recovery-intent schemas and status
components do not constitute a completed manual or automatic DR executor.

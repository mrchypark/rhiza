# Managed SQL/KV/Graph HA

Status: unreleased final explicit append-only profile candidate. Local focused
tests are not final CI, provider, or production qualification. The support
table states the intended boundary, not proof that runtime enforcement is
complete.

## Profile support

| Profile | Policy | HA startup contract |
|---|---:|---|
| Local | Local | Existing Local contract; unchanged |
| Legacy managed SQL | 7 | Existing contract; unchanged |
| Managed SQL/KV | 8 | Existing contract; unchanged |
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
Legacy-7, SQL/KV-8, and Local signatures, defaults, eligibility, and tests stay
unchanged.

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
operator startup / policy / bootstrap / repair authority
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
bypass operator authority at startup, repair, policy, or membership boundaries,
and is not a request-latency or Kube-request-count claim.

An uncertain mutation may have committed. Keep its original kind, raw ID,
payload, and known slot; use exact recovery to resolve the canonical retained
original. Do not resubmit it, generate a new ID, or treat missing evidence as
proof of no effect. Recovery itself adds zero application proposals and does
not refresh retention or clear poison. A duplicate submission may be assigned
a later slot than the canonical first receipt.

The public managed Host requires `before_ack` object-store durability. An
asynchronous success ACK beyond the remote archive head is outside this
contract; a certified-only or uncertain result must not be reported as a
durable ACK.

## NoPVC and recovery boundary

With no PVC, startup reconstructs the verified SQLite/native Graph pair from
the selected immutable checkpoint and replays the contiguous certified suffix
through the registered local WAL's captured certified ACK tip. Checkpoint
selection requires the exact QCKP, actual archive certificate/history, full
object hashes, and pair validation; a CURRENT locator is a hint, not authority.
Missing roots, identity/prefix drift, incomplete installation, poison, or
ambiguous evidence fail closed rather than creating a new baseline.

Object data is append-only. Online delete GC is not part of this profile. Pod
API deletion is not proof that an isolated old process ended or that outstanding
object-store work completed. This candidate does not qualify whole-cluster
disaster recovery or fencing every arbitrary network partition; those remain
explicit gaps.

## Qualification status

Focused Go normal/race checks cover candidate APIs, actual Core/WAL effects,
original receipts, public pair publication/restart, and control admission.
They do not replace final-head Actions or the three object-store provider-SDK checks,
native Rust/C ABI execution, Pod replacement, or scoped Chaos Mesh
qualification. Emulator or unit success is not production-cloud parity. No
release-version support or full HA pass is claimed.

# C FFI / Go contract appendix

This appendix makes the public JSON and Go-facing contracts needed to reproduce the C bridge explicit. It applies to commit `008f8e576ad76425182e10a69b987f10cfb48b2a`. Canonical sources are [Go facade](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/rhiza.go), [network request types](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/network/server.go), [graph types](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/network/graph.go), [value types](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/internal/types/value.go), and [membership types](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/network/membership.go).

Review status: corrected and independently rechecked for the findings raised; see
[c-ffi-go-egm-review-2026-10-30.md](c-ffi-go-egm-review-2026-10-30.md).

## Go configuration and narrow facade

`rhiza.Config` JSON uses Go exported names (no JSON tags). `DataDir` is required. Defaults applied by `Open`: absent `ClusterID` becomes `cluster-a`; non-local absent `BindAddr` and `PeerAddr` each become `127.0.0.1:0`. Local mode disables peer networking and the recovery operator. All fields are accepted by FFI strict decoding:

```text
bool: Local, EnableReconfiguration, ObjStoreInsecure
string: ClusterID, NodeID, DataDir, BindAddr, PeerAddr, AdminToken, PeerToken,
  ObjStoreEndpoint, ObjStoreBucket, ObjStoreProvider, ObjStoreDir, ObjStorePrefix,
  ObjStoreRegion, ObjStoreAccessKey, ObjStoreSecretKey, ObjStoreSessionToken,
  ObjStoreServiceAccount, ObjStoreAzureTenantID, ObjStoreAzureClientID,
  ObjStoreAzureClientSecret, ObjStoreAzureStorageAccount,
  ObjStoreAzureStorageAccountKey, ObjStoreAzureConnectionString,
  ObjStoreAzureUserAssignedID
integer: ObjStoreRetries, CheckpointTailBytes, CheckpointRetainCount, MaxWALBytes,
  MaxConcurrentReads, MaxLongPollReads
duration (Go duration integer nanoseconds): ObjStoreSyncInterval, ObjStoreBatchDelay,
  ObjStoreGCInterval, ObjStoreGCGracePeriod, CheckpointInterval
typed: Members []Member, Learner *Member, ObjStoreDurability, LocalGraphNodePropertyIndexes
```

`DB.Close` is Go-idempotent (`sync.Once`) and returns its first shutdown error on every later call. `Ready` means local recovery/catch-up, not available quorum. `Handler` and `ServeHTTP` expose—but do not listen on—the normal HTTP API. `Open` creates no public HTTP listener. Context controls all public per-operation calls; callers must not mutate request strings/slices/maps until a mutation method returns. `GraphReachable` is snapshot-local and ordered distance then node ID. `ObjectStoreStats` returns `(stats,false)` when unavailable.

Public Go constants visible through these types include consistency `local`/`linearizable`; request kind `sql`/`kv`/`notify`/`graph`; request state `committed`/`rejected`/`unknown_or_expired`; object-store provider `filesystem`/`s3`/`gcs`/`azure`; durability `async`/`before-ack`; and adapter error codes `invalid_request`, `request_conflict`, `not_ready`, `overloaded`, `durability_unavailable`, `commit_unknown`, `quorum_unavailable`, `method_not_allowed`, `not_found`.

## Operation request schemas

All names below are lower snake case JSON tags. Base64 means standard JSON base64 string produced by Go `[]byte`; missing optional values follow Go `omitempty` only where that concrete type declares it.

| Type / operations | Required fields | Optional/default-bearing fields |
| --- | --- | --- |
| `ExecuteRequest` / SQL operations | `request_id` | legacy `sql`, `args`, `want_rows`, `require_one`; `statements` array may coexist with legacy fields. Each statement requires `sql` and can carry `args`, `want_rows`, exact `expected_rows_affected`, exact `expected_returned_rows`, and `output_refs`. |
| `QueryRequest` / `query` | `sql` | `args`; `consistency` chooses local or linearizable behavior |
| `KVGetRequest` / `kv_get` | `key`/`Key`; `consistency`/`Consistency` when supplied | The untagged exported Go fields decode case-insensitively, so direct FFI accepts both spellings. Rust sends `key` and `consistency`; there is no FFI/HTTP capitalization incompatibility. |
| `KVMutationRequest` / put/delete/CAS | `request_id`, `key` | `value` base64, `expected` base64, `expected_exists`, `ttl_ms`; operation selects put/delete/CAS semantics. |
| `GraphCommand` / `graph_execute` | `request_id` | `cypher`, `args`, `events`, `stream_offset`, `stream_trim`; it does not require Cypher when a stream subcommand is used. |
| `GraphQueryRequest` / `graph_query` | `cypher` | `args`, `consistency` |
| `GraphReachableRequest` / reachable | `start_label`, `start_property`, `start_value`, `edge_type`, `result_property` | `node_label`, `node_filters`, `max_depth`, `max_results`, `max_scanned_edges`, `max_bytes`, `require_applied_slot` |
| `GraphStreamReadRequest` / read | none at JSON type level | `stream`, `after_sequence`, `limit` (zero defaults to 100; >1,000 rejected), `wait_ms` (above 30,000 rejected), `consistency`. Stream wait is distinct from the enclosing FFI call deadline. |
| `GraphStreamOffsetRequest` / offset/set | `stream`, `consumer` | `request_id`, `sequence`, `consistency`; `request_id` is necessary for a replicated set by semantic validation. |
| `GraphStreamTrimRequest` / trim | `request_id`, `stream`, `through_sequence` | none |
| `RequestStatusRequest` / status | `kind`, `request_id` | none |
| `NotifyCommand` / publish | `request_id`, `topic`, `payload` base64 | none |

### Exact nested object shapes

`SQLStatement` is `{sql:string,args?:array,want_rows?:bool,expected_rows_affected?:int64,expected_returned_rows?:int64,output_refs?:SQLStatementOutputRef[]}`. An output ref is `{arg_index:int,statement_index:int,column_name?:string,column_index?:int}` and replaces a null positional argument with a prior single-row statement result column. `ExecuteRequest` accepts mixed legacy top-level fields and `statements`: a nonempty statement list supplies the SQL and arguments validated and executed; otherwise the legacy fields supply them. `ExecuteReturning` and `ExecuteReturningOne` reject nonempty `statements`. Unused legacy fields still participate in `SQLFingerprint` through `EncodeSQLBatchItem`; removing or changing them on a same-ID retry can cause `request_conflict`. The request never accepts unrecognized aliases.

`GraphCommand.events` contains `{stream:string,kind:string,payload:any}`. `stream_offset` is `{stream:string,consumer:string,sequence:uint64}` and `stream_trim` is `{stream:string,through_sequence:uint64}`. A graph query is `{cypher:string,args?:object,consistency?:string}`. Reachability has no hidden defaulted bounds at the bridge: `max_depth`, `max_results`, `max_scanned_edges`, and `max_bytes` are explicit unsigned JSON integers in the public request type, as are its required start/edge/result fields. Its response is `{nodes:[{value:any,distance:uint32}],start_found:bool,scanned_edges:uint,applied_slot:uint64,consensus_tip:uint64}`.

Graph stream read response is `{records:[{sequence:uint64,kind:string,payload:any}],applied_slot:uint64,consensus_tip:uint64}`. Offset response is `{sequence:uint64,found:bool,applied_slot:uint64,consensus_tip:uint64}`. These fields are not optional in Go JSON output. `KVGetResponse` is `{found:boolean,value?:base64,applied_slot:uint64,consensus_tip:uint64}`; Go omits `value` for empty bytes because of `omitempty`. Rust returns `None` when `found` is false and, when true, treats a missing `value` as an empty byte vector (otherwise base64-decodes it).

The bridge rejects unknown fields before underlying validation. Semantic validation happens in the public Go call and returns its mapped code; the exact field/tag forms above remain part of this contract rather than being delegated to a validator.

## Response schemas and result semantics

All responses are nested in the bridge `data` envelope. Mutation responses (`ExecuteResponse`, `KVMutationResponse`, `GraphExecuteResponse`, `MutationReceipt`) serialize a receipt with `slot`, `status`, `applied`, `error_code`, `rows_affected`, `last_insert_id`, and `retry_through_slot` where populated. `status:"committed"` and `status:"rejected"` are valid returned outcomes; callers must not treat the latter as a transport error. `execute_returning` additionally returns `statements`, each with `rows_affected`, `last_insert_id`, `columns`, and `rows`.

`QueryResponse` / graph query results carry columns and rows plus applied/tip metadata when the concrete result has it. SQL/graph JSON arguments and rows accept JSON scalar/object/array values as the underlying public types permit. Rust’s typed query struct treats missing/null `columns` and `rows` as empty arrays.

`kv_get` response is `{found:boolean,value?:base64,applied_slot:uint64,consensus_tip:uint64}`. Rust returns `None` when `found` is false; when true it treats missing `value` as an empty byte vector and otherwise base64-decodes it. `notify_recv` is `{payload:base64}`. `request_status` includes `state:string`, `tip:uint64`, and optional `receipt`; status expiry is represented by `unknown_or_expired`, not inferred from missing JSON.

`graph_reachable` requires nonzero bounds and enforces depth at most 64, results at most 10,000, scanned edges at most 1,000,000, and encoded node bytes at most 16 MiB. Node-local property index list is capped at 64. These are the values exported through `rhiza.go` at this commit.

`MutationReceipt` always has `slot`, `status`, and `retry_through_slot`; `error_code`, `rows_affected`, `last_insert_id`, and `applied` use `omitempty`. The only documented status strings are `committed` and `rejected`; documented application rejection codes include `execution_failed`, `precondition_failed`, and `result_retention_full`. A receipt’s `slot` can still be meaningful when its status is rejected.

## Go API signatures and behavior

The narrow exported `DB` surface is: `Open(context.Context, Config) (*DB,error)`, `Close() error`, `Ready() bool`, `Handler() http.Handler`, `ServeHTTP(http.ResponseWriter,*http.Request)`, `Execute`, `ExecuteReturning`, `ExecuteReturningOne`, `Query`, `KVGet`, `KVPut`, `KVDelete`, `KVCAS`, `GraphExecute`, `GraphQuery`, `GraphReachable`, `GraphChanges`, `GraphStreamRead`, `GraphStreamOffset`, `SetGraphStreamOffset`, `TrimGraphStream`, `NotifyPublish`, `NotifySubscribe`, `NotificationDrops`, `RequestStatus`, and `ObjectStoreStats`. Every operational method except no-context state access takes `context.Context`. Cancellation before mutation submission can return the context error directly. After queue submission or proposal registration/join, cancellation can return `CommitUnknownError`, whose unwrap preserves both `ErrCommitUnknown` and the context cause; detached work may continue. FFI prioritizes `commit_unknown` over `timeout` or `canceled`. Preserve the original request ID and identical intent on retry: replay protection holds only while the materialized tip is at most `retry_through_slot`; `unknown_or_expired` does not distinguish an unsubmitted, pending, or expired request, and retry after expiry can execute again.

`ExecuteReturningOne` commits only when exactly one returned row is produced. The `ExecuteReturningMap` helpers reject nil DB/mapper, require committed status and exactly one statement; their `SQLRow.Columns`/`Values` copy data, `Value` checks bounds, and `Named` requires exactly one column of that name. `Migrate` is separate from FFI dispatch: it accepts contiguous versions beginning at one, nonempty names/statements, no statement args/return rows/output references, and uses deterministic checksums/request IDs. `NewReplicaMember` strips a voter secret into a public peer identity; `PeerPublicKey` derives public identity from cluster/node/token.

## FFI versus HTTP request differences

For most structs the FFI uses the same tagged JSON as the normal HTTP adapter, but it does not use HTTP route-specific helper structs. `KVGetRequest` is decoded directly into untagged exported Go fields, whose matching is case-insensitive; therefore both `key`/`consistency` and `Key`/`Consistency` are accepted, with no FFI/HTTP capitalization incompatibility. HTTP also authenticates membership endpoints and uses a 64 KiB membership request reader; direct FFI membership dispatch has neither HTTP authentication nor that HTTP body reader, but retains the 16 MiB FFI cap and underlying lifecycle checks. Do not infer equivalent authorization merely because both reach the same Go membership methods.

Normal HTTP errors are objects such as `{code,error,request_id?,slot?,retry_through_slot?}` accompanied by HTTP status; FFI errors are only `{error:{code,message}}`, losing HTTP request/slot metadata. Conversely, FFI has C buffer ownership and registry-handle errors that HTTP cannot produce. HTTP method errors and route absence produce `method_not_allowed`/`not_found`; no FFI operation produces those codes.

## Membership wire contract

`MembershipChange` has `operation_id`, `cluster_id`, `expected_config_id`, `expected_abort_slot` (defaults to zero), optional nonempty `remove`, optional `add`, and optional `fence`. An add member uses `node_id` (Rust field `id` is renamed), optional `url`/`log_url`, required `peer_url`, `public_key` (base64 public Ed25519 identity), and `wal_identity`. The private peer token never appears. Fence contains `node_id`, `wal_identity`, `workload_uid`, `confirmed`, and `evidence`. Status exposes `node_id`, `cluster_id`, `config_id`, voter IDs, `wal_identity`, `voting`, `pending`, and `abort_slot`; it is observational only and never itself authorizes fencing.

## Rust type mapping

`Error { code:String, message:String }` is deserialized directly from bridge errors and implements `Display` as `code: message`. `MutationReceipt` fields are all optional to detect incomplete native responses; `require_committed` accepts only `status == "committed"`, turns `rejected` into the receipt error code or `rejected`, and treats absent/unknown status as `invalid_response`. `ExecuteReturningResult` flattens a receipt and defaults absent/null statements to empty. `RequestStatus` requires `state` and `tip`, receipt optional.

`Db::open`/`open_from_env` require `data.handle` as unsigned 64-bit; a missing/wrong handle is `invalid_response`. `start_operator` requires response `address` string. `call<T>` converts JSON serialization/deserialization failures to `invalid_response`, except oversize input is preflight `invalid_request`. Rust avoids an input null pointer by passing a mutable vector; C still owns only output buffers. Its FFI declaration makes `RhizaBuffer` `repr(C)` and uses `usize` for C `size_t`, so the native archive and Rust process must have compatible target ABI.

## Build flags and unsupported combinations

The Rust crate has no Cargo features. `build.rs` observes `DOCS_RS`, `RHIZA_NATIVE_LIB_DIR`, `GOEXPERIMENT`, `GOFLAGS`, `CC`, CGO C/C++/link flags, `GOTOOLCHAIN`, `GOROOT`, `SDKROOT`, `MACOSX_DEPLOYMENT_TARGET`, `GOOS`, `GOARCH`, and `GOWORK` for rebuild tracking. It forces `CGO_ENABLED=1` and `GOWORK=off` for its Go command. It rejects all OSes except macOS and linux-gnu, any target not equal to host, and architectures other than aarch64/x86_64. `RHIZA_NATIVE_LIB_DIR` must contain exactly named `librhiza_ffi.a`; it does not enable cross compilation. An unhandled Go runtime/engine panic can terminate the Rust host process.

## Known contract gaps

This appendix has not independently executed the ABI, compared generated C headers, or re-derived every nested public network/type struct field from first principles. Exact nested tags and validation limits remain anchored to the listed canonical files and require the parent’s independent Goldfish review/verification before being treated as reviewed.

No runtime, generated header, or production source was changed while producing this appendix.

# Rhiza C FFI and embedded wrappers: reverse specification

This describes the behavior at commit `008f8e576ad76425182e10a69b987f10cfb48b2a`, not a proposed interface. The C archive exposes an in-process, synchronous bridge to one or more Go `rhiza.DB` instances. No exported function opens a public application HTTP listener.

Source of record: [C bridge](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/cmd/rhiza-ffi/main.go), [Go facade](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/rhiza.go), and [Rust wrapper](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/sdk/rust/src/lib.rs).

Review status: corrected and independently rechecked for the findings raised; see
[c-ffi-go-egm-review-2026-10-30.md](c-ffi-go-egm-review-2026-10-30.md).

## Boundary and wire format

Every non-void C call returns UTF-8 JSON in an allocated `RhizaBuffer`:

```c
typedef struct { void *data; size_t len; } RhizaBuffer;
```

The bytes are exactly one of `{"data": <JSON>}` or `{"error":{"code":string,"message":string}}`. `data` is present even when its value is `null`; `error` is omitted on success. Callers must parse JSON, not infer failure from an empty buffer. All request JSON is strict: unknown object fields, malformed values, trailing JSON, a `null` call request, and missing required envelope fields fail. Numbers use Go `json.Number`, preserving integral JSON spelling through decoding rather than first converting through floating point.

At every byte-pointer boundary, `len` above 16 MiB is rejected. A null pointer with nonzero length also becomes the same invalid input. A zero length accepts an empty byte slice, then ordinary JSON/address validation decides the result. The size cap applies to each FFI request before dispatch, including base64 expansion; it is not a database result-size cap.

## C ABI

| Export | Arguments | Result and effect |
| --- | --- | --- |
| `RhizaOpen` | `void *data, size_t len`: JSON object matching Go `rhiza.Config` field names/tags | Opens an embedded DB and returns `data.handle` (`uint64`, never zero) or an error. JSON `null` is rejected. |
| `RhizaOpenFromEnv` | none | Reads current process `RHIZA_*` configuration, serializes it, then has `RhizaOpen` behavior. |
| `RhizaCall` | `uint64_t handle, void *data, size_t len, uint64_t timeout_ms` | Dispatches `{operation,request}` against a live handle. |
| `RhizaStartOperator` | `uint64_t handle, void *address, size_t len` | Starts a private recovery-only TCP HTTP server and returns `data.address`, the actual bound address. |
| `RhizaClose` | `uint64_t handle` | Removes the handle, cancels it, stops owned subscriptions/operator, waits for admitted activity, closes the Go DB, and returns `data:null`. |
| `RhizaFree` | a returned `RhizaBuffer` | Calls C `free(buffer.data)`. It returns no status. |

`RhizaOpen`, `RhizaOpenFromEnv`, `RhizaCall`, `RhizaStartOperator`, and `RhizaClose` transfer one C-allocated response buffer to the caller. Call `RhizaFree` exactly once for every returned non-null or null buffer after the bytes are no longer read; do not use `free`, retain the pointer after freeing, or pass an invented buffer. Inputs are copied during the call, so the caller retains and may release input storage when the C function returns.

## Handle lifecycle, concurrency, and cancellation

The registry starts allocating at 1 and monotonically increments. Allocation fails with `handle_exhausted` if the next value is zero or `UINT64_MAX`; closed handles are deleted and never reused. The observable state machine is:

```text
absent --open--> live --close claims/removes--> closing --activity drain + DB.Close--> absent
 live --start operator once--> live+operator
 live --subscribe (0..64 total pending+live)--> live+subscription(s)
```

Calls and operator requests admitted before closing count as active. `RhizaClose` first removes the handle, marks it closing and cancels its root context; later calls see `invalid_handle`. It cancels subscription receives, waits for all active work, then calls Go `DB.Close`. A concurrent caller that acquired the entry before the close may return `canceled`, `timeout`, or its ordinary result. Close is not idempotent at C level: a second or racing close whose registry lookup loses returns `invalid_handle`.

`timeout_ms == 0` means 30,000 ms. Positive values are milliseconds and must not exceed `MaxInt64 / time.Millisecond`; excess returns `invalid_request` with `timeout_ms overflows duration`. Timeout cancels the call context, not necessarily all underlying cleanup at that exact deadline. A close cancellation reports `canceled` when the delegated method returns `context.Canceled`. Native callers may invoke a handle concurrently. The bridge serializes lifetime accounting, not general database operation ordering.

## Calls and operation matrix

`RhizaCall` requires `{"operation":string,"request":object}`. Except for
`ready` and `object_store_stats`, the request object is decoded strictly into
the listed Go public type; an operation with `{}` requires exactly an empty
object. `ready` and `object_store_stats` do not decode `request`, so any
present non-null JSON value passes that part of dispatch. The enclosing request
cannot be null. Success payloads are marshaled from the listed Go return value.

| Operation | Request / success `data` | Semantics |
| --- | --- | --- |
| `execute` | `ExecuteRequest` / `ExecuteResponse` | Replicated SQL mutation. |
| `execute_returning` | `ExecuteRequest` / `ExecuteResponse` | Replicated SQL mutation with bounded returned rows. |
| `query` | `QueryRequest` / `QueryResponse` | SQL read. |
| `kv_get` | `KVGetRequest` / `KVGetResponse` | Key/value read. |
| `kv_put`, `kv_delete`, `kv_cas` | `KVMutationRequest` / `KVMutationResponse` | Replicated key/value mutations. |
| `graph_execute` | `GraphCommand` / `GraphExecuteResponse` | Replicated graph mutation. |
| `graph_query` | `GraphQueryRequest` / `GraphResult` | Graph read. |
| `graph_reachable` | `GraphReachableRequest` / `GraphReachableResult` | Bounded local snapshot traversal; ordered by distance then node ID. |
| `graph_stream_read` | `GraphStreamReadRequest` / `GraphStreamReadResponse` | Reads replicated named stream after cursor. |
| `graph_stream_offset` | `GraphStreamOffsetRequest` / `GraphStreamOffsetResponse` | Reads durable consumer offset. |
| `set_graph_stream_offset` | `GraphStreamOffsetRequest` / `null` | Stores durable offset. |
| `trim_graph_stream` | `GraphStreamTrimRequest` / `null` | Deletes through requested sequence. |
| `request_status` | `RequestStatusRequest` / `RequestStatusResponse` | Reports a locally retained materialized outcome or `unknown_or_expired`; provides no consensus barrier or proof of the configured before-ACK durability barrier. An identical mutation retry rechecks that durability barrier; status lookup does not. |
| `ready` | any present non-null JSON request / boolean | Does not decode its request; local recovery/catch-up readiness, not a quorum probe. |
| `object_store_stats` | any present non-null JSON request / `{stats:<object>,available:boolean}` | Does not decode its request; `stats` is Go’s zero-value representation when unavailable. |
| `membership_change`, `membership_abort` | `MembershipChange` / `null` | Operator/recovery membership controls, not normal application mutation APIs. |
| `membership_status` | `{}` / membership status object | Observational membership snapshot. |
| `notify_publish` | `NotifyCommand` / `MutationReceipt` | Replicated notification publication. |
| `notify_subscribe` | `{topic:string}` / `{subscription_id:uint64}` | Creates a live pull subscription. |
| `notify_recv` | `{subscription_id:uint64}` / `{payload:base64}` | Waits for one queued payload under call deadline. |
| `notify_unsubscribe` | `{subscription_id:uint64}` / `null` | Stops and removes that subscription. |
| `notification_drops` | `{}` / unsigned integer | Node-local dropped-delivery counter. |

Unknown operations return `invalid_request`, `unknown operation: <value>`. The Go public aliases in [rhiza.go](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/rhiza.go) identify the concrete request/response schemas; their FFI-relevant JSON field contract is reproduced in the appendix.

## Notification and operator behavior

Each handle permits at most 64 live plus being-created subscriptions. Subscription IDs start at 1, are handle-local, and fail `handle_exhausted` at zero or `UINT64_MAX`. `notify_recv` on an unknown/removed ID returns `invalid_subscription`; waiting after unsubscribe/close returns `canceled`. Delivery is live, pull-based, at-most-once, and has no replay. Saturation can drop deliveries; reconcile from durable application state. Raw concurrent receive and unsubscribe have no post-unsubscribe delivery barrier: a payload already queued may still be returned.

Operator address bytes are trimmed as whitespace and must leave a nonempty string. A local-mode DB rejects the operator. Each handle may start it once; a second call gets `invalid_request`. The handler admits requests only while live; a request admitted during closing remains active until its handler returns. `RhizaClose` gives `http.Server.Shutdown` 30 seconds, then forcibly closes it if needed. It is recovery-only: its routes are from Go `DB.OperatorHandler`, not the normal application HTTP API.

## Errors, commit ambiguity, and HTTP distinction

Bridge-created errors use exact codes `invalid_request`, `invalid_handle`, `closed`, `handle_exhausted`, `invalid_subscription`, and `overloaded`. Delegated Go errors map in priority order: `commit_unknown`, `timeout` (`context.DeadlineExceeded`), `canceled`, `invalid_request`, `not_ready`, `quorum_unavailable`, `request_conflict`, `durability_unavailable`, `overloaded`, otherwise `internal`. `message` is the current Go error string and is not a stable symbolic API beyond bridge-authored cases.

A mutation error or timeout does not prove rollback. Preserve and query the original request ID with `request_status`; never manufacture a new ID merely to retry an ambiguous mutation. Cancellation before submission can return a context error directly; after queue submission or proposal registration/join it can carry commit ambiguity while preserving the context cause, mapped to `commit_unknown` by FFI, and detached work may continue. Retry with the original ID and identical intent within the receipt retention window (materialized tip at most `retry_through_slot`); after expiry, retry can execute again. Status lookup alone does not prove configured before-ACK durability; an identical mutation retry rechecks that barrier. Successful mutation transport can still carry a rejected `MutationReceipt`; inspect its status/error code.

Go `DB.Handler`/`ServeHTTP` expose an optional HTTP adapter, whereas this FFI dispatch invokes Go methods directly. HTTP adds method/path routing, HTTP status codes, and its own up-to-`MaxHTTPBodyBytes` envelope; FFI has the 16 MiB bridge cap, no URL/method, always embeds success/error in JSON, and exposes the recovery operator only through `RhizaStartOperator`. `DB.Open` starts private peer machinery but neither interface opens a public listener.

## Rust facade

`rhizadb::Db` owns the opaque `u64` and is not `Clone`; it is shareable through `Arc<Db>`. `close(&mut self)` turns repeated Rust close into `Ok(())`, sets `closed` even when native close fails, and `Drop` calls close while discarding any error. Post-close safe-wrapper methods return `{code:"closed",message:"Rhiza DB is closed"}` without C entry. Calls block the thread; async users must use a blocking executor facility.

The Rust buffer guard always invokes `RhizaFree` after copying/decoding, including decode failures. Rust encodes outgoing `serde_json::Value` before C entry and rejects an encoded payload over 16 MiB. It requires a response envelope with exactly one `data` or `error`; malformed native output becomes `invalid_response`. `Config` is an object of Go exported field names: `new` supplies `DataDir`; builders supply `NodeID`, `ClusterID`, `BindAddr`, `PeerAddr`, and `local` supplies `Local:true`; `set_option` passes any named field and native strict decoding rejects unknown names. Debug prints only field names.

Typed conveniences default their SQL/KV/graph reads to `consistency:"local"`; `call` is the supported escape hatch for remaining operations and defaults to 30,000 ms. Byte KV and notification data are base64 on the JSON boundary. `NotificationSubscription<'a>` borrows its `Db`, requires `&mut self` for receive/unsubscribe, and its Drop attempts unsubscribe; that prevents the raw concurrent receive/unsubscribe race in safe Rust.

The wrapper’s named conveniences map exactly as follows. `execute` and `execute_returning` produce SQL envelopes with `request_id`, `sql`, and `args`. `query`, `kv_get`, and `graph_query` deliberately add local consistency. `kv_put`, `kv_delete`, and `kv_cas` select their operation string; put/CAS encode byte inputs in standard base64, and CAS sets `expected_exists:true`. `graph_execute`, `graph_reachable`, `request_status`, membership calls, and `call` serialize the supplied type/value without an alternate Rust protocol. `ready` and `notification_drops` call with `{}`. These wrappers do not retry automatically or synthesize request IDs.

`NotificationSubscription::recv_timeout` calls `notify_recv` with its ID and decodes the returned base64 payload. Its documented timeout error is the bridge’s `timeout` code. `unsubscribe` is Rust-idempotent after its `closed` flag is set, but a native unsubscribe failure leaves the flag false and lets Drop attempt it again. `Db::start_operator` and `close` require `&mut self`, so safe Rust cannot call either concurrently with another method through the same unshared `Db` value; shared `Arc<Db>` deliberately exposes only methods requiring `&self`.

Rust’s `Config::local` merely supplies `Local:true`; it does not clear previously supplied peer/listener fields. The Go config validation decides whether such combinations are acceptable. `open_from_env` has no Rust configuration argument and therefore observes environment only at that opening call; later environment changes do not alter the existing native handle.

## Observable limits and non-guarantees

The FFI response buffer is allocated only after dispatch serializes a response. A serialization failure is converted to `internal`/`encode response: ...`; if even that error envelope cannot serialize, the hardcoded fallback is `{"error":{"code":"internal","message":"encode error"}}`. An empty Go response byte slice would be returned as `{NULL,0}`, though all normal public calls produce an envelope. `RhizaFree` is safe at the C allocator level for a null `data` pointer as supplied by a returned empty buffer, but double-free remains caller misuse.

Operator listener startup occurs before the asynchronous serving goroutine. A successful result means TCP bind succeeded, not that a later request will authenticate or recover a node. If `Server.Shutdown` times out or fails during close, the bridge invokes `Server.Close` and then continues draining/closing; the shutdown failure is not returned unless `DB.Close` later returns one. The listener’s configured address is opaque string input to `net.Listen("tcp", ...)`; port zero returns the kernel-selected address.

The bridge’s 30-second default applies per `RhizaCall`, including the `notify_recv` long poll. Membership changes have a second underlying 30-second lifecycle timeout, so the shorter of caller deadline and that operational deadline wins. This does not make application mutations universally bounded: cancellation propagation and engine cleanup are delegated to the underlying Go call.

## Reimplementation-critical source map

The following source regions establish the details that must remain coupled in a compatible replacement:

| Canonical source | Contract supplied |
| --- | --- |
| [cmd/rhiza-ffi/main.go:30](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/cmd/rhiza-ffi/main.go#L30) | Bridge limits, handle entry state, registry monotonicity, and JSON envelope structs. |
| [cmd/rhiza-ffi/main.go:87](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/cmd/rhiza-ffi/main.go#L87) | Every exported C name, signature, and ownership transfer point. |
| [cmd/rhiza-ffi/main.go:141](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/cmd/rhiza-ffi/main.go#L141) | Open / environment-open handle allocation and exhaustion behavior. |
| [cmd/rhiza-ffi/main.go:185](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/cmd/rhiza-ffi/main.go#L185) | Operator admission, local-mode rejection, one-listener state, and listener startup. |
| [cmd/rhiza-ffi/main.go:231](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/cmd/rhiza-ffi/main.go#L231) | Per-call timeout selection, strict outer envelope validation, and error translation. |
| [cmd/rhiza-ffi/main.go:266](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/cmd/rhiza-ffi/main.go#L266) | Close ordering: registry deletion, cancellation, operator/subscription cleanup, drain, then Go close. |
| [cmd/rhiza-ffi/main.go:368](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/cmd/rhiza-ffi/main.go#L368) | Exact operation strings and direct Go method target for each string. |
| [cmd/rhiza-ffi/main.go:526](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/cmd/rhiza-ffi/main.go#L526) | Subscription capacity, identity, and close race behavior. |
| [cmd/rhiza-ffi/main.go:603](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/cmd/rhiza-ffi/main.go#L603) | Envelope serialization and error-code priority order. |
| [rhiza.go:78](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/rhiza.go#L78) | Config field set, public constants, and exported aliases. |
| [rhiza.go:213](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/rhiza.go#L213) | Go open defaults and `DB.Close`/ready semantics. |
| [rhiza.go:305](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/rhiza.go#L305) | HTTP exposure distinction and all DB method forwarding contracts. |
| [sdk/rust/src/lib.rs:1](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/sdk/rust/src/lib.rs#L1) | C declaration ABI, owned buffer guard, `Db` behavior, typed wrapper mapping. |
| [sdk/rust/build.rs:1](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/sdk/rust/build.rs#L1) | Supported targets, archive override, toolchain invocation, and link flags. |

This source map is evidence only. The behavior in the preceding sections is the reconstruction target; it must not be weakened to a source-reference requirement for users of the replacement.

## Explicit exclusions

The C archive does not provide an API to borrow a Go `DB` pointer, inject a Go context, enumerate handles, resurrect a closed handle, reset the handle counter, or free anything other than a returned response buffer. It does not expose a callback ABI for notifications; receiving is always a separate blocking `RhizaCall`.

No FFI operation starts the normal `DB.Handler` server, selects an HTTP port for application traffic, or supplies HTTP authorization headers. Hosts that need the normal HTTP adapter must create a listener and route to `DB.Handler` in Go; hosts using this C ABI instead pass every request through the JSON operation dispatch.

The Rust layer is a synchronous owner wrapper, not an async runtime integration, connection pool, transaction manager, or process isolation boundary. It cannot convert an in-process Go panic into a recoverable Rust `Error`.

## Native build contract

The crate’s build script supports macOS and `linux-gnu` only, only host builds (`TARGET == HOST`), and only `aarch64`→Go `arm64` or `x86_64`→Go `amd64`. `DOCS_RS` skips native build only for documentation. Ordinary builds choose adjacent repository source when it is a Rhiza checkout, otherwise bundled `native`; they run `go build -buildmode=c-archive` with `CGO_ENABLED=1`, `GOWORK=off`, target GOOS/GOARCH, unless `RHIZA_NATIVE_LIB_DIR` names a directory containing `librhiza_ffi.a`. The override avoids Go but must be ABI-matched to the crate release. Linux links `pthread`, `dl`, `m`, `resolv`; macOS links `resolv`, CoreFoundation, and Security. Go 1.27.3+ and a C compiler are required for source builds.

## Scope limits

This document deliberately does not reverse-specify database execution, replication, recovery, SQL, graph, or object-store internals. It specifies their public Go calls only where the C ABI/wrapper exposes their boundary, cancellation, ownership, and wire behavior.

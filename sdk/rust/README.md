# Rhiza Rust SDK

`rhizadb` is a synchronous embedded SDK. It builds Rhiza's local Go C archive
from the native source bundled in the crate, so consumer builds need Rust, Go
1.27+, a C compiler, and a macOS or Linux GNU host (ARM64 or x86-64). Go module
dependencies are fetched by the Go toolchain during that build; no precompiled
native archive is downloaded.

docs.rs builds API documentation without the native archive (`DOCS_RS=1` only).
That mode is documentation-only; normal builds always require the Go and C
toolchains above.

```toml
[dependencies]
rhizadb = "0.12.0"
serde_json = "1"
```

For a repository checkout, use `rhizadb = { path = "sdk/rust" }`. Maintainers refresh
the bundled native tree with `./scripts/prepare-native.sh` before packaging.

## Migration from `rhizadb` 0.4

Version 0.5 replaces the earlier Rust implementation with the Rhiza Go FFI
bridge and therefore has no compatibility layer. It requires Go 1.27+ and a C
toolchain at consumer build time. Update imports to `rhizadb`, create a
`Config`, open `Db`, and use the request/response types in this crate.

Run `cargo run --example embedded` from this directory. `Db` closes on drop;
call `close()` with exclusive `&mut self` ownership to handle close errors.
It is shareable through
`Arc<Db>`, but it is not `Clone`. Calls block the current thread, so async
applications should use their runtime's blocking API.

The native Go runtime is in-process: an unhandled native engine/runtime panic
can terminate the host process. A timeout cancels the call context but is not a
hard upper bound on native cleanup, and `Drop` cannot report a close failure.

`Config::new(path)` sets Go's `DataDir`. Use `node_id`, `cluster_id`,
`bind_addr`, `peer_addr`, and `set_option("GoFieldName", value)` for other
Go `rhiza.Config` fields. `Debug` prints field names only and never values.

`execute` and other mutations return `MutationReceipt`; always call
`require_committed()` because an execution-level rejection is a valid API
response. `Error.code` preserves `commit_unknown`. A mutation timeout or response
decoding error does not imply rollback: look up `request_status` and reuse the
same request ID when retrying. The SDK never retries with a new ID automatically.
The `call` method is an escape hatch for supported native
operations not wrapped by a convenience method. Calls use a 30-second timeout
by default; use `call_timeout` to set a different value. Convenience SQL, KV,
and graph reads request local consistency. Pass an explicit `consistency` value
through `call` when a supported operation needs a linearizable read.

Opening a DB starts the engine's private voter-peer endpoint, but no public HTTP
listener. Notification callbacks are not exposed by this SDK. Graph stream
operations use `call`: `graph_stream_read`, `graph_stream_offset`,
`set_graph_stream_offset`, and `trim_graph_stream`, with the corresponding
[Go request fields](../../rhiza.go).

SQL and graph parameters use `serde_json::Value`. Integer values preserve Rust
`i64`/`u64` ranges accepted by JSON and Go's `UseNumber`; do not pass important
integers through `f64`. KV byte values are base64-encoded by the SDK and accept
NUL bytes. The complete JSON envelope, including base64 expansion, is limited
to 16 MiB before it enters the native bridge. Responses use the Go engine's
existing result limits. JSON encoding, C buffers, and owned Rust results incur
copies; this is not a zero-copy interface or a process-wide memory limit.

Set `RHIZA_NATIVE_LIB_DIR` to a directory containing a compatible
`librhiza_ffi.a` built from the exact matching Rhiza bridge/source version to
skip the default Go build. Cross-compilation is deliberately unsupported rather
than guessed.

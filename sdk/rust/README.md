# Rhiza Rust SDK

`rhizadb` is a synchronous embedded SDK. It builds Rhiza's local Go C archive
from the native source bundled in the crate, so consumer builds need Rust, Go
1.27+, a C compiler, and a macOS or Linux GNU host (ARM64 or x86-64). The Go
dependency closure is vendored inside the crate, so the build resolves every
module from the crate and needs no network access; no precompiled native archive
is downloaded. Each release also attaches a stripped archive per supported
target, so `RHIZA_NATIVE_LIB_DIR` can replace the Go build; see
[Prebuilt native archives](#prebuilt-native-archives).

docs.rs builds API documentation without the native archive (`DOCS_RS=1` only).
That mode is documentation-only; normal builds always require the Go and C
toolchains above.

```toml
[dependencies]
rhizadb = "0.15.2"
serde_json = "1"
```

For a repository checkout, use `rhizadb = { path = "sdk/rust" }`. Maintainers
refresh the bundled native tree with `prepare-native.sh`; releases stage it with
`stage-crate.sh`.

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
listener. `notify_publish` and `notify_subscribe` provide bounded pull-based
subscriptions. `NotificationSubscription::recv_timeout` returns binary payloads
and `Drop` unsubscribes. Delivery is live, at-most-once, and has no replay;
slow subscribers can lose events and `notification_drops` is a node-level
counter. Notifications provide no security or cache-coherence guarantee, so the
host must reconcile missed events from durable application state. Concurrent raw
FFI receive and unsubscribe calls have no strict post-unsubscribe delivery
barrier: an already queued payload may still be returned. The typed Rust API
requires `&mut self` for receive and unsubscribe, preventing that race in safe
Rust. Graph stream
operations use `call`: `graph_stream_read`, `graph_stream_offset`,
`set_graph_stream_offset`, and `trim_graph_stream`, with the corresponding
[Go request fields](../../rhiza.go).

## Recovery operator for embedded hosts

An embedded host that runs the Rhiza recovery operator opens its generation
from the same process `RHIZA_*` configuration as the server binary:

```rust
let mut db = rhizadb::Db::open_from_env()?;
let recovery_addr = db.start_operator("127.0.0.1:9091")?;
```

`start_operator` serves `GET /recovery/status` plus authenticated
`GET /recovery/probe`, `POST /recovery/archive`, `GET /membership/status`,
`POST /membership/change`, and `POST /membership/abort`; application SQL, KV,
graph, readiness, and metrics routes return 404. Keep the bound address private to the operator. It can be
started once and stops with `Db::close` or `Drop`. The environment is read at
open time, so recovery generations require the host process to restart with
the operator-provided `RHIZA_*` values.

SQL and graph parameters use `serde_json::Value`. Integer values preserve Rust
`i64`/`u64` ranges accepted by JSON and Go's `UseNumber`; do not pass important
integers through `f64`. KV byte values are base64-encoded by the SDK and accept
NUL bytes. The complete JSON envelope, including base64 expansion, is limited
to 16 MiB before it enters the native bridge. Responses use the Go engine's
existing result limits. JSON encoding, C buffers, and owned Rust results incur
copies; this is not a zero-copy interface or a process-wide memory limit.

## Prebuilt native archives

Every release attaches stripped archives built from that release tag, with a
`.sha256` beside each one:

```
librhiza_ffi-<version>-aarch64-apple-darwin.a
librhiza_ffi-<version>-x86_64-apple-darwin.a
librhiza_ffi-<version>-x86_64-unknown-linux-gnu.a
librhiza_ffi-<version>-aarch64-unknown-linux-gnu.a
```

Download the archive matching the crate version and your Rust target, verify it,
and point `RHIZA_NATIVE_LIB_DIR` at a directory that holds it as
`librhiza_ffi.a`:

```sh
version=0.15.2
target=aarch64-apple-darwin
asset=librhiza_ffi-${version}-${target}.a
base=https://github.com/mrchypark/rhiza/releases/download/v${version}

curl -fLO "$base/$asset"
curl -fLO "$base/$asset.sha256"
shasum -a 256 -c "$asset.sha256"     # sha256sum -c on Linux
mkdir -p /opt/rhiza-native
mv "$asset" /opt/rhiza-native/librhiza_ffi.a
export RHIZA_NATIVE_LIB_DIR=/opt/rhiza-native
```

With `RHIZA_NATIVE_LIB_DIR` set, the build does not invoke Go and does not need
the Go toolchain. The archive must come from the same release as the crate
version, because the bridge ABI is only guaranteed between matching versions.
Nothing is fetched automatically: supplying an archive is the consumer's
explicit choice, so removing a release asset later removes only that
convenience. Cross-compilation is deliberately unsupported rather than guessed;
build for the host target, or supply an archive for the target you link for.

Repository integration guides: [host-managed shutdown](https://github.com/mrchypark/rhiza/blob/main/docs/embedded-lifecycle.md),
[notification reconciliation](https://github.com/mrchypark/rhiza/blob/main/docs/notification-integration.md), and
[cache expiry](https://github.com/mrchypark/rhiza/blob/main/docs/cache-expiry.md). These guides cover host lifecycle, live notification
delivery, and application-controlled cleanup using the existing APIs.

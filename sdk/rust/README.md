# Rhiza Rust SDK

`rhizadb` is a synchronous embedded SDK. It builds Rhiza's local Go C archive
from the native source bundled in the crate, so consumer builds need Rust, Go
1.27.4+, a C compiler, and a macOS or Linux GNU host (ARM64 or x86-64). The Go
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
rhizadb = "0.27.4"
serde_json = "1"
```

`rhizadb` 0.27.4 is the latest tagged release and includes the
`execute_detailed` API described under [Managed SQL hosts](#managed-sql-hosts).

For a repository checkout, use `rhizadb = { path = "sdk/rust" }`. Maintainers
refresh the bundled native tree with `prepare-native.sh`; releases stage it with
`stage-crate.sh`.

Maintainers must publish the complete verified stage, not `sdk/rust` directly.
Unset `DOCS_RS` and `RHIZA_NATIVE_LIB_DIR`, and retain the new evidence directory:

```sh
unset DOCS_RS RHIZA_NATIVE_LIB_DIR
sh sdk/rust/scripts/publish-verified-crate.sh HEAD /path/to/new-release-evidence
```

The script checks the complete native inventory, source bytes and modes against
an exact-commit stage, builds the extracted package without a prebuilt archive,
then compares a fresh publish stage with that verified package. It publishes
with Cargo's configured credential provider and compares registry bytes after
publication. A post-publication mismatch or readback failure cannot undo a
published crate; inspect the registry before attempting another publication.
`--no-verify`, `DOCS_RS`, or direct `cargo publish` bypass this release process
and must not be used for releases.

## Known issues

`rhizadb` 0.15.2 and earlier fail to open a database when the data directory is
a relative path, for example `Config::new("./rhiza-rust-data")`:

```
Error { code: "internal", message: "rebuild materializer: init schema: sqlite3:
unable to open database file: lstat /rhiza-rust-data: no such file or directory" }
```

The engine built a `file://` URL from the relative path, which resolved the
database against the filesystem root instead of the working directory. 0.15.3
resolves the path first, so relative and absolute data directories both work. On
0.15.2 and earlier, pass an absolute path.

The Rhiza server has the same defect for a relative `RHIZA_DATA_DIR`, which is
its default `./rhiza-data`; the fix is in the same release.

## Migration from `rhizadb` 0.4

Version 0.5 replaces the earlier Rust implementation with the Rhiza Go FFI
bridge and therefore has no compatibility layer. It requires Go 1.27.4+ and a C
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

Since 0.17.0, use
`Db::open(Config::new("./data").node_id("local").local())` for a local-only
database. It opens no peer
transport or listener; `start_operator` is rejected. SQL, KV, graph, receipts,
and WAL recovery use the existing engine. Listener addresses, members, tokens,
and learner/reconfiguration settings must be omitted. There is no replication.
Without object storage, the existing local WAL size limit still applies.

For Local object-storage backup, the Rust facade passes documented Go config
fields through `set_option`; it does not provide typed object-store builders.
The engine streams backup history and can restore an empty `DataDir` during
open. Use the same storage identity and ensure only one process owns the archive
at a time. See the [Local backup and recovery contract](https://github.com/mrchypark/rhiza/blob/v0.27.4/docs/embedded-object-storage.md),
including durability, archive compatibility, and filesystem ownership limits.

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

`graph_reachable` keeps its existing request struct and method signature for
source compatibility. Call `graph_reachable_directed(request,
GraphReachableDirection::Incoming)` on either `Db` or `ManagedDb` to send the
typed `direction:"incoming"` field.

## Recovery operator for embedded hosts

An embedded host that runs the Rhiza recovery operator opens its generation
from the same process `RHIZA_*` configuration as the server binary:

```rust
let mut db = rhizadb::Db::open_from_env()?;
let recovery_addr = db.start_operator("127.0.0.1:9091")?;
```

`start_operator` serves `GET /recovery/status` plus authenticated
`GET /recovery/probe`, `POST /recovery/archive`, `POST /recovery/drain`, `GET /membership/status`,
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

## Managed SQL hosts

For an application running inside a Rhiza-managed Pod, use the separate managed
handle. It loads the verified runtime profile, Pod identity, bootstrap handoff,
and optional repair handoff; cluster and Pod identity are taken from the
process environment. The default paths are the installed paths:

```toml
[dependencies]
rhizadb = { path = "/path/to/rhiza/sdk/rust" }
serde_json = "1"
```

Replace the path with the local checkout containing unreleased `main`.

```rust
use rhizadb::{
    ExecuteReturningResult, ManagedDb, ManagedExecuteRequest, ManagedQueryRequest,
    ManagedRecoveryResult, ManagedSqlStatement, ManagedSqlStatementOutputRef, MutationKind,
};
use serde_json::{json, Value};
use std::error::Error;

fn execute_managed(
    db: &ManagedDb,
    request: &ManagedExecuteRequest,
) -> Result<ExecuteReturningResult, Box<dyn Error>> {
    let result = match db.execute_detailed(request) {
        Ok(result) => result,
        Err(failure) if failure.error().code == "commit_unknown" => {
            let original = match failure.mutation_context() {
                Some(context)
                    if context.kind() == MutationKind::Sql
                        && context.request_id() == request.request_id.as_bytes() =>
                {
                    context.request_id()
                }
                Some(_) => return Err("mutation context changed".into()),
                None => request.request_id.as_bytes(),
            };
            match db.recover_request(MutationKind::Sql, original)? {
                ManagedRecoveryResult::Sql { request_id, result }
                    if request_id == request.request_id.as_bytes() =>
                {
                    result
                }
                _ => return Err("original outcome remains unresolved".into()),
            }
        }
        Err(failure) => return Err(failure.into_error().into()),
    };

    let receipt = &result.receipt;
    receipt.require_committed()?;
    let slot = receipt
        .slot
        .filter(|slot| *slot != 0)
        .ok_or("committed receipt has no slot")?;
    let retry_through = receipt
        .retry_through_slot
        .ok_or("committed receipt has no retry boundary")?;
    if retry_through < slot
        || receipt
            .error_code
            .as_deref()
            .is_some_and(|code| !code.is_empty())
    {
        return Err("committed receipt failed slot/retention validation".into());
    }
    Ok(result)
}

fn main() -> Result<(), Box<dyn Error>> {
    let mut db = ManagedDb::open_from_env()?;
    let insert = ManagedExecuteRequest::sql(
        "insert-42",
        "INSERT INTO app VALUES (?)",
    )
    .args([json!("value")]);
    execute_managed(&db, &insert)?;
    let _rows = db.query(&ManagedQueryRequest::new("SELECT value FROM app"))?;

    let transaction = ManagedExecuteRequest {
        request_id: "parent-and-child".into(),
        sql: None,
        args: vec![],
        want_rows: false,
        require_one: false,
        statements: vec![
            ManagedSqlStatement {
                args: vec![json!("parent")],
                want_rows: true,
                expected_returned_rows: Some(1),
                ..ManagedSqlStatement::new("INSERT INTO parent(name) VALUES (?) RETURNING id")
            },
            ManagedSqlStatement {
                args: vec![Value::Null, json!("child")],
                output_refs: vec![ManagedSqlStatementOutputRef {
                    arg_index: 0,
                    statement_index: 0,
                    column_name: Some("id".into()),
                    column_index: None,
                }],
                ..ManagedSqlStatement::new("INSERT INTO child(parent_id, name) VALUES (?, ?)")
            },
        ],
    };
    execute_managed(&db, &transaction)?;

    db.close()?;
    Ok(())
}
```

Use `execute_detailed` for managed SQL mutations when the original outcome
matters. `execute` remains source-compatible and returns only `Error`, so it
does not expose `mutation_context`. On `commit_unknown`, keep the original request
ID and intent. Validate the context when present, then use `recover_request` to
resolve the exact engine-retained original; do not submit a replacement write as
recovery. If the context is missing, use the known original kind and request ID
with `recover_request`. Treat a mismatched context as unresolved. A mismatched
recovery result is not a rollback confirmation either.

Before accepting a receipt, require a committed status, a nonzero slot, and a
retention boundary at or after that slot. Do not require the recovered canonical
slot or boundary to equal the `commit_unknown` context: recovery returns the
canonical first receipt, while a duplicate submission may have a later slot. The
unknown attempt's slot and boundary are diagnostic retention hints, not canonical
receipt identity. `retry_through_slot` is the inclusive retention boundary for
the same request ID and intent; a material tip beyond it means the retained
outcome is expired, not that the original did not commit. Exact recovery submits
no write.

The complete wrapper above also shows the transaction form: `statements` runs as
one managed write, and an output reference fills a `NULL` argument from an earlier
statement's returned row. Route both single-statement and transaction requests
through the same exact-recovery helper.

`ManagedDb` deliberately has no generic operation escape hatch, local mode,
or legacy operator listener or membership methods. It exposes typed SQL, KV,
and Graph methods. The released managed HA profiles are explicit append-only 9/10,
generation 0, with `before-ack` durability; see the
[managed request contract](../../docs/managed-internal-request-api.md) and
[managed Graph HA boundary](../../docs/managed-graph-ha.md).
`ready()` checks managed admission; `recover_pending()` resolves the retained
original pending request, while `recover_request` selects an exact kind and ID.
If the per-handle hint is missing or ambiguous, use `recover_request` with the
known original kind and request ID; the engine must still retain that original.
Writes are never automatically retried with a new request ID. A
`commit_unknown` failure does not establish rollback. Preserve the original kind,
raw request ID, intent, and any known slot. Use `recover_request(kind, request_id)`
to resolve that exact retained original; do not resubmit the mutation as recovery.
Calls use the native bridge's 30-second default timeout.

## Prebuilt native archives

Every release attaches stripped archives built from that release tag, with a
`.sha256` beside each one:

```
librhiza_ffi-<version>-aarch64-apple-darwin.a
librhiza_ffi-<version>-x86_64-apple-darwin.a
librhiza_ffi-<version>-x86_64-unknown-linux-gnu.a
librhiza_ffi-<version>-aarch64-unknown-linux-gnu.a
```

`sdk/rust/scripts/fetch-native.sh` downloads the archive for a crate version and
target, verifies it against the published checksum, and writes it as
`librhiza_ffi.a`. The target defaults to the host triple, and the destination to
`$RHIZA_NATIVE_LIB_DIR` or `./.rhiza-native`:

```sh
curl -fsSLO https://raw.githubusercontent.com/mrchypark/rhiza/v0.27.4/sdk/rust/scripts/fetch-native.sh
sh fetch-native.sh 0.27.4
export RHIZA_NATIVE_LIB_DIR="$(pwd)/.rhiza-native"
```

For containers, pass the host target and destination explicitly:

```sh
sh fetch-native.sh 0.27.4 x86_64-unknown-linux-gnu /opt/rhiza-native
```

The same files can be fetched by hand: download
`librhiza_ffi-<version>-<target>.a` and its `.sha256` from
`https://github.com/mrchypark/rhiza/releases/tag/v<version>`, verify the
checksum, and place the archive as `librhiza_ffi.a`.

With `RHIZA_NATIVE_LIB_DIR` set, the build does not invoke Go and does not need
the Go toolchain. The archive must come from the same release as the crate
version, because the bridge ABI is only guaranteed between matching versions.
Nothing is fetched automatically: supplying an archive is the consumer's
explicit choice, so removing a release asset later removes only that
convenience. Cross-compilation is unsupported even with a prebuilt archive:
Rust HOST must equal TARGET. The explicit target argument selects an archive
for a native host build; it does not enable cross-compilation.

Repository integration guides: [host-managed shutdown](https://github.com/mrchypark/rhiza/blob/main/docs/embedded-lifecycle.md),
[notification reconciliation](https://github.com/mrchypark/rhiza/blob/main/docs/notification-integration.md), and
[cache expiry](https://github.com/mrchypark/rhiza/blob/main/docs/cache-expiry.md). These guides cover host lifecycle, live notification
delivery, and application-controlled cleanup using the existing APIs.

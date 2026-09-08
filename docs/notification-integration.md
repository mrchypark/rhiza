# Notification integration

Rhiza v0.12.3 exposes replicated binary publication and live subscriptions in Go,
HTTP and Rust. A successful publication receipt certifies the mutation, not
receipt by every subscriber. Subscriptions have no replay. Keep one subscription
alive across receives; recreating it for each receive introduces blind intervals.
Rust calls block, so an async host must run receiving and DB work on its blocking
facility and bound concurrency. Use finite receive timeouts to observe host shutdown.
A timeout is an idle receive, not a reason to terminate the listener.

## Reconcile missed hints

Use notifications as hints to re-read authoritative state:

1. Open a subscription, then read a snapshot. Specify `consistency: "linearizable"`
   through Rust `Db::call("query", ...)` when current quorum state is required;
   the convenience `query` method reads locally.
2. Commit state before publishing its hint. These are separate mutations; a crash
   between them can suppress the hint. Retry each uncertain mutation with its
   original request ID and unchanged payload within the receipt retention window.
3. Re-read state on each hint, after reconnect, and periodically while connected.
   Schedule the periodic read independently of incoming traffic. A reconnect or
   missed final notification may have no subsequent hint to expose the gap.
4. Treat `notification_drops()` as node-level diagnostics, not a per-subscriber
   sequence or proof that nothing was missed. Publication without subscribers
   does not produce a replayable event or necessarily increment this counter.
5. Stop admissions, signal the receiving worker, await its finite receive timeout
   and join it, drop its subscription, then explicitly close the database.

This gives eventual reconciliation only. It does not guarantee immediate session
revocation, authorization freshness, exactly-once processing, or delivery of every
event. Security decisions that need current state must verify authoritative state
and fail closed if that read is unavailable. If every event must be processed,
use an application-owned transactional outbox with a durable cursor and retention
contract; existing Graph streams are another option for Graph-owned events.

The runnable Rust `missed_notifications_reconcile_from_durable_state` test verifies
slow-subscriber loss, re-reading the latest durable state, disconnected publication,
and no replay on reconnection. Existing FFI tests cover cross-node delivery,
idempotent publication, bounded subscriptions, cancellation and close.

```sh
cargo test --manifest-path sdk/rust/Cargo.toml missed_notifications_reconcile_from_durable_state
```

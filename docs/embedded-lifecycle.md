# Embedded lifecycle

The embedding application owns the Rhiza process lifetime. `rhiza.Open` starts
the private peer endpoint and background work; it does not install signal
handlers or start a public HTTP listener.

## Go

Stop admitting new application work, cancel its request contexts, wait for the
goroutines that use `DB`, then call `Close` and handle its error. This ordering
prevents application work from reaching a database whose files and peer
endpoint are being released.

```go
func run(ctx context.Context) (err error) {
	db, err := rhiza.Open(ctx, config)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, db.Close()) }()

	// Start application work with contexts derived from ctx.
	// On shutdown: stop accepting work, cancel those contexts, and wait for it.
	return serve(ctx, db)
}
```

`DB.Close` is safe to call more than once and returns the same shutdown error.
It flushes and closes Rhiza-owned resources, including the WAL. A close error
means shutdown was incomplete and must be recorded or surfaced by the host;
do not discard it with `defer db.Close()` in long-lived applications.

The standalone `rhiza` server installs `SIGINT` and `SIGTERM` handling because
it owns its process. An embedded library cannot select a host's signal policy:
GUI programs, test runners, supervisors, and applications with several
services need different coordination. Hosts that use OS signals should create
their own `signal.NotifyContext`, pass its cancellation through their worker
contexts, and use the shutdown order above. Rhiza deliberately provides no
separate signal helper.

`SIGKILL`, `os.Exit`, aborts, and power loss cannot run `Close`. Recovery uses
the durable local WAL, but an asynchronous object-store archive or checkpoint
may not include the final acknowledged work. Give a supervised process enough
termination grace time to drain workers and return from `Close`; use
before-ack object-store durability when that remote-copy guarantee is required.

## Rust

`rhizadb::Db::close(&mut self)` calls the Go FFI close operation. Call it from
the application's controlled shutdown path so its `Result` is handled:

```rust
let mut db = rhizadb::Db::open(config)?;
// Stop new work and join tasks that call db first.
db.close()?;
```

`Drop` also attempts close, but cannot report a failure. For an `Arc<Db>`, stop
and join all workers before recovering exclusive ownership with
`Arc::try_unwrap`, then call `close`. Rust calls are synchronous, so async
hosts should run them through their runtime's blocking facility and join those
tasks before shutdown.

At the raw FFI boundary, `RhizaClose` removes the handle from new calls,
cancels active bridge calls, waits for them to drain, then closes the Go DB.
It returns a JSON error if close fails. A Rust `Db` is considered closed after
`close` returns, including when that final close reports an error, so report
the error immediately rather than relying on a later retry.

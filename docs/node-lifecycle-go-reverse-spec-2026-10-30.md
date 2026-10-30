# Rhiza Go node lifecycle reverse specification

## Scope

Current behavior at `008f8e576ad76425182e10a69b987f10cfb48b2a`: startup,
readiness, learner/voter admission, catch-up, Local checkpoint lifecycle, and
shutdown in `pkg/node`. This is current implementation behavior, not a proposal.
Full archive staging is outside scope; only its lifecycle prerequisites appear.
Source references use canonical project paths, not this detached checkout.

`New(*types.ExecutionConfig)` stores the supplied pointer and opens no resource.
`Open(context.Context)` opens Core and private peer transport, without HTTP.
`Start(context.Context)` opens then blocks in `http.Server.Serve` on the public
listener. Local Start fails: `local mode does not support an HTTP listener; use
Open`. Caller-context cancellation asks that HTTP server alone to shut down with
a separate 10-second background deadline; its shutdown error is discarded.
Successful HTTP serving does not call `Node.Shutdown`, and a non-
`http.ErrServerClosed` Serve error returns directly without it. The caller must
call `Node.Shutdown` for engine cleanup on every successful Start return and on
such Serve failures; only listener-creation failure joins `Shutdown`.

## States, readiness, and public access

There is no exported state enum. Externally: new -> opening -> open/not-ready ->
open/ready; Shutdown returns the instance to not-open (it need not erase every
private field). A terminally drained node rejects later Open or
EnrollExistingVoter permanently. The `opened` atomic bit is claimed after static
validation and cleared when shutdown starts.

`Ready()` remains false during startup recovery and catch-up. It does not prove
live quorum. The server is intentionally present while false for recovery record
RPCs, while proposals and learned decisions remain gated. A compaction signal
clears readiness. A transient catch-up error does not clear an already true bit.
Before server creation, `Handler()` and `API()` return `node is not open`.

## Open ordering

`Open` uses shared opening with enrollment false. `EnrollExistingVoter` uses it
with enrollment true. Both reject terminal drain, nil/missing NodeID, and open
state. Once `opened.CompareAndSwap` succeeds, any error defers Shutdown and
returns the original/wrapped error after owned-resource cleanup.

Required order: validate Local GC; connect fencing; reject invalid Local setup;
validate non-Local SQL; validate membership, tokens, storage/read/WAL limits;
claim open; derive node context; lock `DataDir/qlog`; load voter identity;
open/validate storage and WAL; ensure voter identity; create Core/materializer/
server/peer transport; install/verify generation and recovery anchors; recover
and replay; set ready; start periodic/background work. No listener starts before
identity and recovery prerequisites.

Local mode is standalone only: no enrollment, members, learner, reconfiguration,
bind/peer address, peer token, or admin token. Existing Local storage cannot be
opened as non-Local. Pod deletion is not evidence that an isolated process ended
and grants no recovery authority.

## Membership and identity admission

Explicit voter IDs are nonempty and unique. A nonlearner local NodeID must be a
voter. Multi-node admission requires peer token, distinct admin token, and a
nonzero public key for every voter. Managed membership needs at least two
bootstrap voters and admin token. A learner requires bootstrap members,
reconfiguration, a local ID absent from voters, equal Learner.ID, and nonzero
learner public key. A configured nonempty learner WAL identity equals Core WAL
identity.

EnrollExistingVoter is offline pre-registration: it records voter WAL identity
while voters are offline, restores no missing voting state, and starts no
listener. Missing or contradictory voter evidence fails; learner identity never
becomes voter proof.

## Fencing and terminal drain

Fencing is opt-in only when both `RHIZA_FENCE_SOCKET` and `RHIZA_FENCE_TICKET`
are set. Exactly one set fails `supervised fencing requires both socket and
ticket`; Local mode or fewer than two members fails `supervised fencing requires
registered shared-storage voters`. Connection receives cluster and node identity;
connection failure wraps `connect fencing startup gate: %w`.

When supervised, identity registration must bind the WAL nonce and call the
session's `BindWAL` before it writes remote registration. The connection session
starts a watcher after normal background workers start. It intentionally does not
join `wg`, because it can itself invoke shutdown. Transient pending, unavailable,
or network errors retry after 250 ms. A non-retryable supervisor error while the
node context remains live logs and invokes Shutdown without a drain receipt.

On a stop request, the watcher requires `TerminalDrain` to confirm the exact
cluster ID, node ID, and WAL identity. Terminal drain irreversibly sets its
terminal-requested bit, joins shutdown writers, then drains every tracked bucket
mutation and closes the bucket before setting `StorageKnown`. It will acknowledge
only when that result has `StorageKnown`; acknowledgement has a 30-second context,
retries only transiently, and replays the same clean receipt. A lost
acknowledgement never authorizes a fresh process or another recovery operation.
An unconfirmed drain is retained as evidence rather than treated as a successful
process stop.

## Local identity and checkpoint roots

Opening Local data first validates `local-store.json`. The policy is version 1
and binds a 16-byte hexadecimal store UUID plus a SHA-256 configuration digest
over version, cluster, node, Local flag, and graph property indexes. A policy
file must be a nonsymlink regular file at most 4096 bytes. Existing materialized
data without policy, policy/digest mismatch, invalid UUID, or a policy with
materialized data but no WAL manifest fails with preserve-existing-data errors;
only an empty data directory may receive a fresh policy.

A Local root has exactly `descriptor.json`, `sqlite.db`, and `graph.ltdb`; its
descriptor identifies version, store/config digest, config ID, slot, prefix,
`sql+graph/v1` policy, ordered file roles, state hash, and root hash. Capture
requires exactly SQLite plus graph files, each nonempty, and total no larger than
32 GiB. Descriptor JSON is capped at 16 KiB. Publish copies, hashes, syncs, and
renames the root before Core appends prepared-checkpoint state; that durable root
is intentionally recoverable if the later consensus operation fails.
The root digest is SHA-256 of the local-root domain followed by JSON for the
descriptor with `RootHash` and `StateHash` cleared. The state digest is SHA-256
of the local-state domain followed, in file order, by `role`, NUL, `name`, NUL,
the eight-byte big-endian length, `sha256`, and NUL for each file.

## Catch-up

The worker first catches up membership, then loops on deterministic SHA-256
NodeID/round jitter of 900--1100 ms. Compaction wake uses a capacity-one channel
and coalesces callers. Fetch starts at materializer tip plus one, limit 128.
Quorum rounds use 2 seconds; individual incremental pages use 250 ms. Peer order
is deterministic by round group.

Only contiguous responses whose values pass execution-policy validation enter
Core and materializer. A compacted source may invoke live checkpoint/archive
recovery; for voters, only a fresh later quorum round reasserts readiness. Missing quorum is
`quepaxa.ErrQuorumUnavailable`; a source claiming a tip but omitting its required
slot is an error.

A configured non-voting learner instead tries membership peers sequentially
(excluding itself) until one `catchUpPeer` Sync path succeeds, then calls
`EnsureDurableThrough` at the Core tip and replays local decisions. Success of
that complete path lets `observeCatchUp` restore readiness. The not-ready
recovery loop uses the same voter/learner dispatch, including after compacted
source recovery. Learner readiness grants neither voting authority nor quorum
proof. Sources: `pkg/node/node.go`, `catchUpMembership`, `startCatchUp`, and
`observeCatchUp`.

## Local quiescence and recovery evidence

Local reclaim and prepared-checkpoint resume both call `server.Quiesce(ctx)`
before Core/materializer mutation and defer resume, but their error handling is
different. Reclaim wraps its specified cancellation/maintenance failures in
`network.ErrLocalMaintenanceRefused` and defers `FailLocalLifecycle` (which
clears ready) for unexpected lifecycle failure. Resume is startup work: a
`Quiesce` failure, including a raw `ctx.Err()`, propagates directly, and it does
not share reclaim's wrapping or failure-handler defer. `network.ErrNotReady`
remains its sentinel. Durable identity/root evidence stays for recovery.

Archive prerequisite: publish a checkpoint only after its decision archive is
durable. Archive metadata alone never authorizes root deletion. Pins and GC locks
retain evidence until their owning archive protocol completes.

## Ownership, cancellation, and Shutdown

Node owns derived context, peer server/transports, periodic sync, checkpointer,
catch-up worker, Local-GC context, archive, WAL, materializer, bucket, Local
owner, and qlog lock. Caller owns configuration and input context. Cancellation
stops work but does not erase WAL/checkpoint/archive/fence/maintenance evidence.

Shutdown serializes, clears open/ready, cancels Local GC and Local work, closes
server admission, stops checkpointer, cancels context, waits `wg`, closes peer and
transports, stops periodic sync, synchronizes backups/archive, closes archive,
syncs/closes WAL, closes materializer, drains/checks Local maintenance, closes
bucket/owner, then releases qlog lock. Server remains through foreground peer
handler joining. Errors use `errors.Join`.

Archive sync gets 10 seconds and is voter-only under reconfiguration. Local
mutation drain gets 30 seconds, maintenance check 5 seconds, owner close 30
seconds. Drain/maintenance failure poisons the filesystem owner and retains
evidence. Shutdown attempts filesystem-owner close before attempting qlog-lock
release, but joins an owner-close error and still attempts `n.lock.Release()`.
Active-operation timeout or poisoned ownership can therefore retain the
filesystem-owner locks after shutdown returns an error, even though qlog-lock
release was attempted. Sources: `internal/objstore/local_owner.go`,
`LocalFilesystemOwner.Close`; `pkg/node/node.go`, `shutdownEngine`.

## Canonical references

- `/Users/cypark/.codex/worktrees/quepaxa-go-reverse-spec/rhiza/pkg/node/node.go`
- `/Users/cypark/.codex/worktrees/quepaxa-go-reverse-spec/rhiza/pkg/node/voter_identity.go`
- `/Users/cypark/.codex/worktrees/quepaxa-go-reverse-spec/rhiza/pkg/node/fencing_session.go`
- `/Users/cypark/.codex/worktrees/quepaxa-go-reverse-spec/rhiza/pkg/node/generation.go`
- `/Users/cypark/.codex/worktrees/quepaxa-go-reverse-spec/rhiza/pkg/node/local_checkpoint.go`

Review: [corrected and independently rechecked within the recorded scope](node-lifecycle-go-egm-review-2026-10-30.md).

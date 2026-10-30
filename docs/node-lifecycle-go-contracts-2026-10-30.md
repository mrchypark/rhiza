# Node lifecycle Go contracts appendix

Applies to commit `008f8e576ad76425182e10a69b987f10cfb48b2a`. Exact public and
lifecycle-boundary behavior only; archive staging has a separate specification.

## Public API

```go
func New(config *types.ExecutionConfig) *Node
func (n *Node) Open(ctx context.Context) error
func (n *Node) EnrollExistingVoter(ctx context.Context) error
func (n *Node) Start(ctx context.Context) error
func (n *Node) Handler() (http.Handler, error)
func (n *Node) API() (*network.Server, error)
func (n *Node) Ready() bool
func (n *Node) Shutdown() error
func (n *Node) ObjectStoreStats() (objectstore.Stats, bool)
```

Start opens then listens on BindAddr with HTTP read/write/idle timeouts of 30/60/
120 seconds. A listen failure joins `listen: %w` with Shutdown. Caller-context
cancellation invokes only `http.Server.Shutdown` with a separate 10-second
background deadline and discards its error. `http.ErrServerClosed` is
suppressed; a different Serve failure returns directly and neither that path nor
the normal successful path calls `Node.Shutdown`. The caller must invoke
`Node.Shutdown` to clean the opened engine after those paths. Handler/API before
server installation return nil plus `node is not open`. Ready atomically reads
readiness. Shutdown joins cleanup errors and can close partial opening.

## Defaults, limits, and exact validation

Caller owns ExecutionConfig. Opening changes only an empty ObjStoreDurability:
default async, except Local plus any object-store setting defaults before-ack.
Only async and before-ack are valid. Before-ack requires object storage.
Multi-node requires shared S3, GCS, or Azure; filesystem, empty provider, or
ObjStoreDir fails. These providers require bucket. Negative WAL bytes, sync/batch
delay, GC interval/grace, or read limits fail. Read limits are both zero or
MaxConcurrentReads is positive and MaxLongPollReads is from zero through it.

Literal membership errors: `voter member ID is required`, `duplicate voter member
%q`, `local node %q is not a voter member`, `peer token is required for multi-node
clusters`, `admin token must differ from the peer token`, and `voter public key is
required for multi-node cluster member %s`.

## Fencing, voter identity, generation

Fencing connects before lock/data/WAL access. Supervised fencing requires a
registered voter WAL identity or returns `supervised fencing requires registered
voter WAL identity`. The session BindWAL action executes before remote identity
registration write. Cancellation, failure, or mismatch aborts opening and triggers
partial shutdown.

Voter identity loads after qlog lock but before archive replay/listener startup.
ensureVoterIdentity receives WAL, optional bucket, and enrollment mode. Its durable
identity binding cannot be silently recreated after loss and learner identity is
not voter authority.

installGenerationAnchor precedes peer listeners. It is valid only when checkpoint
and archive lineage verify. Failure wraps `restore generation anchor: %w`.
Unbacked current checkpoint returns `checkpoint CURRENT is not backed by certified
recovery evidence`.

### Immutable voter registration JSON

`DataDir/qlog/voter-identity.json` is written atomically (temporary file Sync,
rename, then qlog and data-dir Sync). Its exact schema is:

```json
{
  "version": 2,
  "cluster": "string",
  "node": "string",
  "membership": "string",
  "store": "hex SHA-256",
  "nonce": "64 lowercase hex characters",
  "registered": true,
  "learner_public_key": "hex string (omitted when no learner)"
}
```

Version is exactly 2. `membership` is the recovery membership record; `store` is
SHA-256 of provider, endpoint, bucket, cleaned prefix, and Azure account. Local
load requires a qlog manifest and valid 32-byte nonce. Invalid JSON/version/nonce
or missing manifest wraps `ErrVoterStateLost` and says to preserve original disk.
Remote registration reads are capped below 64 KiB; immutable remote mismatch is
ErrVoterStateLost. Conditional `IfNotExists` upload support is required.

### Local checkpoint JSON

`local-store.json` has JSON tags `version`, `store_uuid`, and `config_digest`.
Each Local `descriptor.json` has tags `version`, `store_uuid`, `config_digest`,
`config_id`, `slot`, `prefix`, `execution_policy`, `files`, `state_hash`, and
`root_hash`. Each files element has `role`, `name`, `length`, `sha256`.
Version is 1; execution_policy is exactly `sql+graph/v1`; file order is exactly
SQLite `sqlite.db`, then graph `graph.ltdb`; root directory contents are exactly
those two files plus descriptor. Descriptor maximum is 16 KiB and total files
maximum is 32 GiB. Any symlink, unexpected entry, nonregular file, hash/length,
root, prefix, config-digest, or role-order mismatch rejects recovery.
`root_hash` is SHA-256 over the literal byte prefix
`rhiza/local-checkpoint-root/v1` followed by one NUL byte, then the bytes from
Go `json.Marshal` of the ordered `localCheckpointDescriptor` struct after
setting `root_hash` and `state_hash` to empty strings. The fields remain present
(they are not omitted), so those final two encoded fields are
`"state_hash":"","root_hash":""`. The marshaled field order is
`version`, `store_uuid`, `config_digest`, `config_id`, `slot`, `prefix`,
`execution_policy`, `files`, `state_hash`, `root_hash`; JSON strings use Go
`encoding/json` escaping, unsigned integers use its JSON number encoding, and
the `files` slice is encoded in its given order (`null` if it is a nil slice,
otherwise an array). A compatible implementation must hash precisely those
marshal bytes rather than a reformatted or key-sorted JSON representation.
The exact struct fields and tags, in declaration (and serialization) order, are:

```go
type localFileDescriptor struct {
    Role   string `json:"role"`
    Name   string `json:"name"`
    Length uint64 `json:"length"`
    SHA256 string `json:"sha256"`
}
type localCheckpointDescriptor struct {
    Version         uint                  `json:"version"`
    StoreUUID       string                `json:"store_uuid"`
    ConfigDigest    string                `json:"config_digest"`
    ConfigID        uint                  `json:"config_id"`
    Slot            uint64                `json:"slot"`
    Prefix          string                `json:"prefix"`
    ExecutionPolicy string                `json:"execution_policy"`
    Files           []localFileDescriptor `json:"files"`
    StateHash       string                `json:"state_hash"`
    RootHash        string                `json:"root_hash"`
}
```

No field has `omitempty`: zero numbers, empty strings, and every file field
remain encoded. Each file object uses `role`, `name`, `length`, `sha256` in
that order. `json.Marshal` emits compact JSON without a trailing newline;
integers are unsigned decimal numbers. Strings escape quotes, backslashes,
and control characters, use the default HTML escapes `\u003c`, `\u003e`,
`\u0026` for `<`, `>`, `&`, and escape U+2028/U+2029 as `\u2028`/`\u2029`.
Other valid Unicode is UTF-8; invalid UTF-8 bytes become U+FFFD replacement
characters. Neither whitespace normalization nor disabling HTML escaping is
compatible. Both resulting hash strings use lowercase hexadecimal SHA-256.

`state_hash` is SHA-256 over the literal byte prefix
`rhiza/local-checkpoint-state/v1` followed by one NUL byte, then each ordered
file's role, NUL, name, NUL, eight-byte big-endian length, SHA-256 text digest,
and NUL. The role/name separators and trailing digest separator are retained
exactly as stated; the digest is its textual field value, not decoded digest
bytes.

### Terminal drain request

The terminal-drain HTTP payload schema is:

```json
{"clusterID":"string","nodeID":"string","walIdentity":"string"}
```

It is POST-only, bearer-authorized, decoded with unknown fields disallowed, body
limited to 16 KiB, and requires exactly one complete JSON value with all strings
nonempty. Bad input is HTTP 400 `invalid_drain_identity`; missing authorization is
HTTP 403 `forbidden`; unconfirmed drain is HTTP 503 `drain_unconfirmed`.

## Recovery and transfer behavior

Certified restore requires matching state hash and Core tip, uses temporary
DataDir/.rhiza-checkpoint-restore-* download storage, and deletes it on return.
Materializer ahead of Core or missing certified materialized decision fails.

Wake clears readiness with nonblocking capacity-one send. Fetch uses
from=material.Tip()+1 and limit=128. Quorum deadline is two seconds; page deadline
is 250 milliseconds; local recorder contributes one quorum success. Values validate
before Core/materializer mutation. Noncontiguous and missing required slot responses
are failures.

## Local checkpoint contract

Unavailable Local maintenance and specified cancellation return wrapped
network.ErrLocalMaintenanceRefused. Capture files are regular, nonsymlink,
nonnegative-size fixed-role files; graph data is mandatory. Combined bytes may not
exceed localCheckpointMaxBytes; overflow says `Local checkpoint exceeds the
combined %d-byte bound`. Slot movement during capture fails.

After durable root publication, Core consensus reclaim owns prefix authority.
Root deletion failure is logged cleanup debt, not an ambiguous successful
checkpoint. Resumption requires exact root, certified prefix, matching config
identity, quiescence, and retained evidence.

## Close order and evidence

Nil resources are skipped. Implemented order: flags, Local cancellation, server,
checkpointer, context, wait; peer/transports; periodic sync; bounded backup/archive
sync; archive; WAL; materializer; local mutation drain/maintenance; bucket/owner;
qlog lock. Errors join in observation order. Mutation drain, maintenance-resolution,
or bucket-close failure poisons Local owner. Persistent evidence is retained for
later recovery diagnosis.

### Terminal-drain execution

`TerminalDrain(ctx, expected)` first honors cancellation. It refuses concurrent
ordinary shutdown (`node shutdown is already in progress`) and requires a live
opened engine with Core, bucket, and config. It constructs observed identity from
config cluster/node plus `core.WALIdentity()` and requires exact equality;
mismatch returns `terminal drain identity mismatch`. The first valid request marks
terminal request, caches identity/result, and starts one goroutine. Later equal
requests wait for that same result but may time out independently.

The goroutine calls `shutdownEngine(false)`, then drains the captured bucket with
an unbounded background context, closes it, sets bucket nil, and sets
`StorageKnown` only if joined shutdown, drain, and close errors are nil. Thus a
shutdown response, Pod deletion, or cached receipt alone never proves complete
process termination or known persistent-mutation outcome.

## Canonical references

- `/Users/cypark/.codex/worktrees/quepaxa-go-reverse-spec/rhiza/pkg/node/node.go`
- `/Users/cypark/.codex/worktrees/quepaxa-go-reverse-spec/rhiza/pkg/node/fencing_session.go`
- `/Users/cypark/.codex/worktrees/quepaxa-go-reverse-spec/rhiza/pkg/node/voter_identity.go`
- `/Users/cypark/.codex/worktrees/quepaxa-go-reverse-spec/rhiza/pkg/node/generation.go`
- `/Users/cypark/.codex/worktrees/quepaxa-go-reverse-spec/rhiza/pkg/node/local_checkpoint.go`

Review: [corrected and independently rechecked within the recorded scope](node-lifecycle-go-egm-review-2026-10-30.md).

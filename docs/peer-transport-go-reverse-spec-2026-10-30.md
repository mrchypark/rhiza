# Peer Transport Go Reverse Specification

Status: implementation-derived behavior at `008f8e576ad76425182e10a69b987f10cfb48b2a`. This is a
current implementation contract, not a proposed protocol or proof. It excludes public HTTP business
handlers.

## Boundary and model

`network.Transport` sends private peer RPCs over persistent raw QUIC connections. Each RPC opens
one bidirectional QUIC stream; it sends one request frame, closes its write side, reads one
response frame, then releases the connection reference. The peer listener is `PeerServer`, distinct
from the public HTTP adapter. Sources:
`/Users/cypark/.codex/worktrees/quepaxa-go-reverse-spec/rhiza/pkg/network/transport.go:50-72`,
`/Users/cypark/.codex/worktrees/quepaxa-go-reverse-spec/rhiza/pkg/network/peer_server.go:31-51`.

The raw transport envelope is **not** a FlatBuffers schema. It is a four-byte unsigned big-endian
length followed by exactly that many FlatBuffers bytes. Zero-length and lengths above 1,048,576 are
rejected before allocation; writes retry short positive writes and fail on `io.ErrShortWrite` for a
zero-byte successful write. Sources:
`/Users/cypark/.codex/worktrees/quepaxa-go-reverse-spec/rhiza/pkg/network/transport.go:1012-1046`.

The request FlatBuffer has identifier `RHZA`; request decode requires 8..1,048,576 bytes,
identifier, bounds preflight, and marker `0x52485a41`. Response decode requires 4..1,048,576 bytes,
bounds preflight, and that same marker, but no file identifier. Decode panics become `invalid peer
frame: ...` or `invalid peer response: ...`. A non-empty response `error` becomes `peer rejected
request: <text>` while retaining the decoded response for error-code classification. Sources:
`/Users/cypark/.codex/worktrees/quepaxa-go-reverse-spec/rhiza/pkg/network/peer_codec.go:13-64`.

## Configuration, identity, and TLS

An unbound transport uses its construction `ClusterID`, `ConfigID`, and member set. After
`BindCore`, slot-bearing Record, Learned, PrepareCheckpoint calls resolve the immutable
configuration for their slot; Sync, ReadTip, StageValue, and FetchValue use the current
configuration. Dynamic children are keyed by immutable config ID. Each child owns a separate
peer-connection map and TLS client session cache, so a changed voter credential cannot reuse an
older connection or 0-RTT ticket. Creating a child after `Close` fails with `peer transport is
closed`. Sources:
`/Users/cypark/.codex/worktrees/quepaxa-go-reverse-spec/rhiza/pkg/network/transport.go:134-211`.

Voter client TLS is TLS 1.3+, with literal ALPN `rhiza-peer-v7`
(`sqlpolicy.PeerALPN` in `internal/sqlpolicy/policy.go:16`), and a session cache
sized to membership.
QUIC uses a 5-second handshake idle timeout, 30-second max idle timeout, 10-second keepalive, and
256 maximum incoming streams. For each target, client `ServerName` is the target node ID and normal
certificate verification is bypassed only to install exact Ed25519 public-key pinning: exactly one
peer certificate, Ed25519 key type, and constant-time equality with a configured key are required.
A voter with a non-empty private peer token presents its own derived certificate. Sources:
`/Users/cypark/.codex/worktrees/quepaxa-go-reverse-spec/rhiza/pkg/network/transport.go:116-132`,
`:236-303`.

`PeerPublicKey(clusterID,nodeID,token)` derives the Ed25519 key from HMAC-SHA-256 keyed by `token`
over `"rhiza-peer-certificate\\x00" || clusterID || 0x00 || nodeID`; it never transmits the token.
The self-signed certificate has the node ID as CN and DNS name, serial from the first 20 bytes of
SHA-256(public key), Unix-epoch `NotBefore`, 2125-01-01 UTC `NotAfter`, digital-signature usage,
and server/client auth usage. Sources:
`/Users/cypark/.codex/worktrees/quepaxa-go-reverse-spec/rhiza/pkg/network/peer_server.go:601-634`.

Server startup requires every member key in a multi-member cluster, a non-empty peer token in a
multi-member cluster, and (when published) a local public key matching the local token-derived key.
A non-empty admin token must differ from the peer token and must not derive any voter public key.
The listener requests, but does not require, a client certificate, preserving admin-only Sync. It
uses the same TLS/QUIC settings plus `MaxIncomingUniStreams=-1`. Sources:
`/Users/cypark/.codex/worktrees/quepaxa-go-reverse-spec/rhiza/pkg/network/peer_server.go:93-149`.

## Connection and stream lifecycle

For a destination, one-slot `gate` serializes connection creation and waits on the caller context.
Unknown IDs yield `unknown node: <id>`; a URL takes `PeerURL` then `URL`, and parsing must yield a
host or returns `invalid peer URL %q`. A dead or absent cached connection is dialed with
`quic.DialAddrEarly`, but every call waits for `HandshakeComplete`; voting RPCs are never accepted
or sent as replayable early data. Active-stream counts prevent a stale connection from closing
until its final borrower releases it. Sources:
`/Users/cypark/.codex/worktrees/quepaxa-go-reverse-spec/rhiza/pkg/network/transport.go:236-303`,
`:460-486`.

Ordinary `call` wraps the caller context in a 5-second timeout. `callQuorum`
(used by SendRecord, ReadTip, and Learned fan-outs) inherits the caller deadline
unchanged. Checkpoint preparation uses 5 minutes. `callContext` invalidates a cached connection only when the request failed without any
response and the context itself is still live. A received `peerErrorQuorum` maps to
`quepaxa.ErrQuorumUnavailable`; `peerErrorCompacted` maps to `quepaxa.ErrCompacted`; retryable
preserves the decoded rejection error. Other server rejections wrap `errPeerRejected`. Connection
and stream errors are wrapped with phase (`connection-gate`, `peer-identity`, `dial`, `handshake`,
`stream-open`, `request-write`, `request-close`, `response-read`, or `response-decode`), elapsed
time, concrete cause type, and `%w` cause. Sources:
`/Users/cypark/.codex/worktrees/quepaxa-go-reverse-spec/rhiza/pkg/network/transport.go:305-395`.

On each stream, caller deadlines become QUIC stream deadlines. Context cancellation asynchronously
cancels read and write with application error 1; final context error overrides any partial
response. The client always cancels its read direction on return. A normal remote stream-close
error 0 is tolerated only when it is remote and the stream context reports it. Sources:
`/Users/cypark/.codex/worktrees/quepaxa-go-reverse-spec/rhiza/pkg/network/transport.go:353-395`.

The server accepts at most 64 concurrent QUIC connections and 1,024 concurrent streams; excess
connections close with code 1 and `peer connection limit reached`, while excess streams cancel both
directions with code 1. Each stream defaults to a 30-second deadline; PrepareCheckpoint extends it
to 5 minutes. `Close` cancels serving, closes the listener, waits for spawned handlers, and, only
for `StartPeerServer`, releases its owned QUIC transport and UDP socket.
`StartPeerServerOnTransport` leaves both caller-owned resources open. Sources:
`/Users/cypark/.codex/worktrees/quepaxa-go-reverse-spec/rhiza/pkg/network/peer_server.go:31-89`,
`:171-278`.

## Authorization and dispatch

The server first reads the bounded frame, requires completed TLS handshake before decoding, then
selects the request configuration: Record by record slot, Learned by decision slot, Sync by current
config when IDs match otherwise by `from`, PrepareCheckpoint by decoded seal index, all others
current. It requires both request cluster ID and request config ID to equal that configuration.
Sources:
`/Users/cypark/.codex/worktrees/quepaxa-go-reverse-spec/rhiza/pkg/network/peer_server.go:225-286`,
`:534-599`.

A voter must be in both historical/request and active/current memberships and present a client
certificate equal to that member's pinned key in both configurations. A removed voter is therefore
unauthorized despite a historical envelope. A non-voter can pass only with constant-time matching
admin token and only for Sync. Otherwise the error is `peer authentication failed`. Sources:
`/Users/cypark/.codex/worktrees/quepaxa-go-reverse-spec/rhiza/pkg/network/peer_server.go:312-335`.

Handler errors become response text. Quorum unavailable, compacted, and
`ErrNotReady`/`ErrOverloaded` respectively receive error codes 1, 2, and 3; other errors have code
0. The server still attempts one framed response, cancels write on framing failure, then closes the
stream. Sources:
`/Users/cypark/.codex/worktrees/quepaxa-go-reverse-spec/rhiza/pkg/network/peer_server.go:279-307`.

## RPC behavior

`Record` requires `record`, decodes a valid proposal, validates non-empty proposal value through
execution policy, and accepts absent or 32-byte reconfiguration hash only. It fixes record
`ConfigID` from the envelope. If `RecordCatchUpThrough` exceeds local tip, it catches up from the
sender first; no transport or self sender gives `record prefix is unavailable`. Then it calls
`core.Record` and returns its Summary. Sources:
`/Users/cypark/.codex/worktrees/quepaxa-go-reverse-spec/rhiza/pkg/network/peer_server.go:336-366`.

`Learned` rejects while not ready, validates decision/proposal/execution policy, and handles
reconfiguration controls specially. Before a control at slot N>1 it catches up through N-1 from
sender, waits for tip N-1, then accepts with `AcceptDecisionHint` if this server is named as a
recorder, otherwise `AcceptDecision`; it always calls `applyDecisions(ctx,N)` before ACK. Thus a
successful learned response means acceptance plus local application, not merely receipt. Sources:
`/Users/cypark/.codex/worktrees/quepaxa-go-reverse-spec/rhiza/pkg/network/peer_server.go:367-451`.

Normal `SendDecision` fans out Learned concurrently, counts local ACK as one, and succeeds at a
strict majority; it returns the first observed error after all replies otherwise
`ErrQuorumUnavailable`. Freeze controls also require every recorder named in the certificate plus a
majority. A terminal membership decision sends to old∪new, requires both old and new quorums, and
requires every newly added voter to acknowledge; a required added-voter failure returns
immediately. Sources:
`/Users/cypark/.codex/worktrees/quepaxa-go-reverse-spec/rhiza/pkg/network/transport.go:545-873`.

`Sync` is admin-readable certified catch-up. `from` and `limit` must be nonzero and limit at most
128. It returns source identity, requested config ID, tip, and the longest prefix of
`DecisionsFrom(from,limit)` whose **encoded response** fits 1 MiB; one first decision that cannot
fit fails `decision <slot> exceeds peer frame limit`. Client validates cluster, source proposer,
and transport config ID before accepting. Sources:
`/Users/cypark/.codex/worktrees/quepaxa-go-reverse-spec/rhiza/pkg/network/peer_server.go:452-486`,
`/Users/cypark/.codex/worktrees/quepaxa-go-reverse-spec/rhiza/pkg/network/transport.go:488-516`.

At the Node boundary, a configured non-voting learner tries peers sequentially
until one Sync catch-up succeeds, then completes `EnsureDurableThrough` and
local replay before readiness is restored. The not-ready recovery loop uses
this same dispatch; voters require quorum catch-up. Single-source learner
readiness grants neither voting authority nor quorum proof. Source:
`/Users/cypark/.codex/worktrees/quepaxa-go-reverse-spec/rhiza/pkg/node/node.go`,
`catchUpMembership`, `startCatchUp`, and `observeCatchUp`.

`ReadIndex` requires `CanParticipateReadIndex` and readiness, then returns current cluster, node
ID, config ID, and tip; client requires all three identities. `StageValue` validates execution
policy and an exact 32-byte hash before `core.StageValue`; `FetchValue` requires that hash, errors
`value is unavailable` if absent, and client SHA-256-checks then copies returned bytes. Sources:
`/Users/cypark/.codex/worktrees/quepaxa-go-reverse-spec/rhiza/pkg/network/peer_server.go:487-531`,
`/Users/cypark/.codex/worktrees/quepaxa-go-reverse-spec/rhiza/pkg/network/transport.go:875-929`.

`VerifyPrefix` is admission-only: it requires nonempty learner ID, key, and through slot; a
64-character canonical lowercase hex WAL identity; local active-voter status; and learner not
already active. It probes the learner using a temporary union transport. The learner must match its
WAL identity, catch up to `from`, produce the exact durable-prefix hash, and return matching
cluster/proposer/config/tip. The proof-parameter request can trigger durable certified
catch-up into the learner; it does not itself alter membership. Sources:
`/Users/cypark/.codex/worktrees/quepaxa-go-reverse-spec/rhiza/pkg/network/transport.go:931-984`,
`/Users/cypark/.codex/worktrees/quepaxa-go-reverse-spec/rhiza/pkg/network/peer_server.go:514-561`.

`PrepareCheckpoint` encodes a seal and concurrently calls each remote voter with the 5-minute
timeout. Local count starts at one and a majority is enough; cancellation returns its own error,
and an exhausted attempt returns `ErrQuorumUnavailable` wrapped with its first error when one
exists. Server decodes a checkpoint seal and delegates preparation. Sources:
`/Users/cypark/.codex/worktrees/quepaxa-go-reverse-spec/rhiza/pkg/network/transport.go:397-458`,
`/Users/cypark/.codex/worktrees/quepaxa-go-reverse-spec/rhiza/pkg/network/peer_server.go:562-576`.

## Bounds and ownership

Before FlatBuffers `UnPack`, every present schema branch is walked, even branches unused by the
operation. The walker caps materialized copied string/vector bytes at 1,048,576 and tables plus
vector slots at 16,384; repeated references are charged repeatedly. Malformed offsets, vtables,
vectors, or budget breaches are converted to malformed-frame errors. This is an
allocation/materialization guard, not a general FlatBuffers verifier. Sources:
`/Users/cypark/.codex/worktrees/quepaxa-go-reverse-spec/rhiza/pkg/network/peer_bounds.go:1-14`,
`:139-264`.

`Close` marks a transport closed, recursively closes dynamic children, closes cached and active
QUIC connections with code 0/`shutdown`, and clears maps. It returns nil and intentionally ignores
close errors. Requests after closure only fail when they need a new dynamic child; direct base
transport use has no independent closed precheck. Sources:
`/Users/cypark/.codex/worktrees/quepaxa-go-reverse-spec/rhiza/pkg/network/transport.go:985-1010`.

See the companion appendix for exact Go declarations, FlatBuffers fields, conversions, defaults,
and error strings: `docs/peer-transport-go-contracts-2026-10-30.md`.

Review: [corrected and independently rechecked within the recorded scope](peer-transport-go-egm-review-2026-10-30.md).

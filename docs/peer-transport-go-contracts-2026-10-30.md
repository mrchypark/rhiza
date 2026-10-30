# Peer Transport Go Contract Appendix

Status: exact boundary and wire notes for implementation commit `008f8e576ad76425182e10a69b987f10cfb48b2a`. Canonical sources are under `/Users/cypark/.codex/worktrees/quepaxa-go-reverse-spec/rhiza`, not the authoring checkout.

## Exported Go boundary

```go
func NewTransport(clusterID types.ClusterID, localID quepaxa.NodeID, config *quepaxa.Cluster, peerToken string) *Transport
func (t *Transport) SetAdminToken(token string)
func NewPeerIdentity(clusterID types.ClusterID, member quepaxa.Member) (PeerIdentity, error)
func NewLearnerTransport(clusterID types.ClusterID, localID quepaxa.NodeID, configID uint, peers []PeerIdentity, adminToken string) *Transport
func (t *Transport) BindCore(core clusterResolver)
func (t *Transport) PrepareCheckpoint(ctx context.Context, seal quepaxa.CheckpointSeal) error
func (t *Transport) FetchDecisions(ctx context.Context, source quepaxa.NodeID, from quepaxa.Slot, limit int) (DecisionsResponse, error)
func (t *Transport) SendRecord(ctx context.Context, to quepaxa.NodeID, request quepaxa.RecordRequest) (quepaxa.Summary, error)
func (t *Transport) SendDecision(ctx context.Context, decision quepaxa.Decision) error
func (t *Transport) ReadTip(ctx context.Context, to quepaxa.NodeID) (quepaxa.Slot, error)
func (t *Transport) StageValue(ctx context.Context, to quepaxa.NodeID, hash quepaxa.ValueHash, value []byte) error
func (t *Transport) FetchValue(ctx context.Context, from quepaxa.NodeID, hash quepaxa.ValueHash) ([]byte, error)
func (t *Transport) VerifyLearner(ctx context.Context, learner quepaxa.Member, through quepaxa.Slot, prefix quepaxa.ValueHash) error
func (t *Transport) Close() error
func StartPeerServer(ctx context.Context, addr string, server *Server, members []quepaxa.Member, peerToken, adminToken string) (*PeerServer, error)
func StartPeerServerOnTransport(ctx context.Context, transport *quic.Transport, server *Server, members []quepaxa.Member, peerToken, adminToken string) (*PeerServer, error)
func StartLearnerPeerServer(ctx context.Context, addr string, server *Server, voters []quepaxa.Member, peerToken, adminToken string, learner quepaxa.Member) (*PeerServer, error)
func (s *PeerServer) Close() error
func (s *PeerServer) Addr() string
func PeerPublicKey(clusterID types.ClusterID, nodeID quepaxa.NodeID, token string) ed25519.PublicKey
```

`Transport` satisfies `quepaxa.Transport`; `PeerIdentity` has `ID quepaxa.NodeID`, `PeerURL string`, and `PublicKey [32]byte`. `NewPeerIdentity` chooses `Member.PeerURL` then `Member.URL`, and rejects any empty cluster ID, member ID, chosen URL, or zero public key with `cluster ID, voter ID, peer URL, and voter public key are required`. Learner construction retains only peer identities and admin token, with no voter token. `BindCore` is intended once before peer RPCs. Sources: `/Users/cypark/.codex/worktrees/quepaxa-go-reverse-spec/rhiza/pkg/network/transport.go:75-149`.

`StartLearnerPeerServer` requires nonempty learner ID equal to local node ID, learner absent from voters, and nonempty learner peer token; errors are `learner ID is required`, `learner ID must match the local node`, `learner is already a voter`, or `learner peer identity token is required`. `Addr` returns listener address. `Close` is idempotent through `sync.Once`. Sources: `/Users/cypark/.codex/worktrees/quepaxa-go-reverse-spec/rhiza/pkg/network/peer_server.go:151-185`.

## Generated schema: `peer.fbs`

The namespace is `peerfb`; root type is `Request`; only Request buffers carry file identifier `RHZA`. Field numbers are stable IDs, not an implication that absent scalar fields are semantically required.

```text
enum Operation: ubyte
  None=0 Record=1 ReservedPropose=2 Learned=3 Sync=4 ReadIndex=5
  StageValue=6 FetchValue=7 PrepareCheckpoint=8 VerifyPrefix=9

Proposal { priority:[ubyte]#0; proposer_id:string#1; hash:[ubyte]#2; value:[ubyte]#3 }
Summary { recorder_id:string#0; step:ulong#1; first_current:Proposal#2;
          aggregate_prior:Proposal#3; reconfiguration_id:[ubyte]#4 }
RecordRequest { slot:ulong#0; step:ulong#1; proposal:Proposal#2 }
Decision { slot:ulong#0; step:ulong#1; proposal:Proposal#2; summaries:[Summary]#3; config_id:ulong#4 }
DecidedValue { slot:ulong#0; hash:[ubyte]#1; value:[ubyte]#2; certificate:[ubyte]#3 }
Request { magic:uint#0; operation:Operation#1; cluster_id:string#2; sender_id:string#3;
          config_id:ulong#4; admin_token:string#5; record:RecordRequest#6; value:[ubyte]#7;
          decision:Decision#8; from:ulong#9; limit:uint#10; hash:[ubyte]#11 }
Response { magic:uint#0; error:string#1; summary:Summary#2; decided:DecidedValue#3;
           cluster_id:string#4; proposer_id:string#5; config_id:ulong#6; tip:ulong#7;
           decisions:[DecidedValue]#8; error_code:ushort#9; value:[ubyte]#10 }
```

Source: `/Users/cypark/.codex/worktrees/quepaxa-go-reverse-spec/rhiza/pkg/network/peer.fbs:1-64`.

## Operation payload and conversion contracts

Every client request sets `magic`, operation, cluster ID, sender ID, config ID, and admin token. `Record` additionally puts slot/step/proposal in `record` and a nonzero reconfiguration ID in `hash`. `Learned` puts encoded decision in `decision`. Sync sets `from` and `limit`; ReadIndex sets nothing else; StageValue and FetchValue place 32-byte hash in `hash` (StageValue also copies `value`); PrepareCheckpoint puts encoded seal in `value`; VerifyPrefix sets `from`, certified-prefix hash in `hash`, and decoded WAL-identity bytes in `value`. Sources: `/Users/cypark/.codex/worktrees/quepaxa-go-reverse-spec/rhiza/pkg/network/transport.go:213-217`, `:488-963`.

Proposal decode requires nonnil value, 32-byte `priority`, 32-byte `hash`, and nonempty `proposer_id`; value itself may be empty. Summary requires nonnil value and nonempty `recorder_id`; `reconfiguration_id` length is either 0 or 32. Decision requires nonnil value and nonzero slot plus valid proposal and every summary. DecidedValue requires nonnil, nonzero slot, and 32-byte hash. Decoding copies byte slices into new Go values. Certificate summaries serialize proposals with `value=nil`; this is deliberate. Sources: `/Users/cypark/.codex/worktrees/quepaxa-go-reverse-spec/rhiza/pkg/network/peer_codec.go:66-194`.

The response `decided` field is unused by the current dispatch; Sync uses `decisions`. `ReservedPropose` and `None` have no handler and fail `unknown peer operation <n>`. Clients validate response source identity for Sync, ReadIndex, and VerifyPrefix; SendRecord also requires Summary recorder ID equal to target. Sources: `/Users/cypark/.codex/worktrees/quepaxa-go-reverse-spec/rhiza/pkg/network/peer_server.go:576-578`, `/Users/cypark/.codex/worktrees/quepaxa-go-reverse-spec/rhiza/pkg/network/transport.go:488-543`, `:875-963`.

## Limits, defaults, and error mapping

| Contract | Exact value / result |
|---|---|
| FlatBuffer payload frame | 1..1,048,576 bytes (`maxPeerFrame = 1<<20`) |
| decoded request lower bound | 8 bytes plus `RHZA` identifier |
| decoded response lower bound | 4 bytes; no identifier required |
| materialized bytes per decoded frame | at most 1,048,576 |
| materialized tables + vector slots | at most 16,384 |
| `call` RPC deadline | parent deadline shortened to 5 seconds |
| `callQuorum` RPC deadline (SendRecord, ReadTip, Learned fan-outs) | caller-controlled; no transport-added timeout |
| checkpoint RPC / server stream deadline | 5 minutes |
| ordinary server stream deadline | 30 seconds |
| inbound connections / streams | 64 / 1,024 server admission; QUIC max incoming streams 256 |
| Sync limit | 1..128; response page may be shorter by encoded-byte fit |
| hash, priority, WAL identity | SHA-256/hash/priority: 32 bytes; WAL identity: exactly 64 canonical lowercase hex characters |
| server failure code | quorum=1, compacted=2, retryable (`ErrNotReady` or `ErrOverloaded`)=3, otherwise 0 |

Sources: `/Users/cypark/.codex/worktrees/quepaxa-go-reverse-spec/rhiza/pkg/network/peer_codec.go:13-64`, `/Users/cypark/.codex/worktrees/quepaxa-go-reverse-spec/rhiza/pkg/network/peer_bounds.go:15-25`, `/Users/cypark/.codex/worktrees/quepaxa-go-reverse-spec/rhiza/pkg/network/peer_server.go:23-29`, `:279-307`, `/Users/cypark/.codex/worktrees/quepaxa-go-reverse-spec/rhiza/pkg/network/transport.go:26-29`.

Important exact server errors include `cluster identity mismatch`, `peer authentication failed`, `record is required`, `invalid reconfiguration ID`, `invalid decisions range`, `decision <slot> exceeds peer frame limit`, `invalid value hash`, `value is unavailable`, `invalid durable-prefix request`, `learner WAL identity mismatch`, `learner prefix source is unavailable`, `durable prefix mismatch`, `checkpoint seal is required`, and `unknown peer operation <n>`. Core and execution-policy errors propagate unchanged in response text. Sources: `/Users/cypark/.codex/worktrees/quepaxa-go-reverse-spec/rhiza/pkg/network/peer_server.go:312-578`.

## State and ownership summary

```text
constructed -> [BindCore optional] -> usable
usable --operation(slot/current)--> configuration child or base transport
per target: gate -> cached/live connection -> handshake complete -> active++
active stream: open -> write frame -> close write -> read frame -> decode -> release
failure without response and live context -> invalidate cache; final release closes stale idle conn
Close -> mark closed -> close children/connections -> clear maps
```

The parent `Transport` owns dynamically created configuration children and closes them. Each peer connection's active reference map is protected by its mutex; creation is serialized by its one-place gate. The temporary union transport used by a terminal decision and the temporary learner probe each close before return. `PeerServer` owns listener and spawned handler goroutines; only `StartPeerServer` owns and closes its UDP/QUIC transport. Sources: `/Users/cypark/.codex/worktrees/quepaxa-go-reverse-spec/rhiza/pkg/network/transport.go:172-211`, `:236-303`, `:460-486`, `:749-873`, `:931-963`, `:985-1010`, `/Users/cypark/.codex/worktrees/quepaxa-go-reverse-spec/rhiza/pkg/network/peer_server.go:53-89`, `:171-223`.

Cancellation is not converted to a retry. It blocks or aborts gate/dial/handshake/stream work through the supplied context; an after-function cancels stream directions and final `ctx.Err()` wins. Cache invalidation after a failed no-response call does not replay that RPC. Fan-out calls use child cancellable contexts but current quorum-return paths defer cancellation while goroutines may still publish into buffered result channels. This bounds sends by participant count and avoids receiver leaks. Sources: `/Users/cypark/.codex/worktrees/quepaxa-go-reverse-spec/rhiza/pkg/network/transport.go:236-395`, `:397-458`, `:545-873`.

Review: [corrected and independently rechecked within the recorded scope](peer-transport-go-egm-review-2026-10-30.md).

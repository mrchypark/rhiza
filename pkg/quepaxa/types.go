package quepaxa

import (
	"bytes"
	"encoding/json"
	"fmt"
)

var leaderScheduleMagic = []byte("QLDR1\x00")
var readBarrierMagic = []byte("QRDB1\x00")

const ReadBarrierNonceSize = 16

// Slot is a monotonically increasing consensus position.
type Slot uint64

func (s Slot) String() string { return fmt.Sprintf("slot(%d)", uint64(s)) }

// NodeID uniquely identifies a cluster member.
type NodeID string

// ValueHash is the SHA-256 hash of a proposed value.
type ValueHash [32]byte

// Member describes one fixed cluster member.
type Member struct {
	ID      NodeID `json:"node_id"`
	URL     string `json:"url"`
	PeerURL string `json:"peer_url,omitempty"`
	LogURL  string `json:"log_url"`
	Token   string `json:"token"`
}

// Cluster describes the fixed membership used for the lifetime of a Core.
type Cluster struct {
	ConfigID uint     `json:"config_id"`
	Members  []Member `json:"members"`
}

func (c *Cluster) QuorumSize() int { return len(c.Members)/2 + 1 }

func (c *Cluster) MemberSet() map[NodeID]Member {
	members := make(map[NodeID]Member, len(c.Members))
	for _, member := range c.Members {
		members[member.ID] = member
	}
	return members
}

// SlotValue identifies a decided value without copying its payload.
type SlotValue struct {
	Slot Slot
	Hash ValueHash
}

func (value SlotValue) String() string {
	return fmt.Sprintf("slot=%d hash=%x", value.Slot, value.Hash[:8])
}

// DecidedValue is the durable value and quorum certificate bound to a slot.
type DecidedValue struct {
	Slot        Slot            `json:"slot"`
	Hash        ValueHash       `json:"hash"`
	Value       []byte          `json:"value"`
	Certificate json.RawMessage `json:"certificate,omitempty"`
}

// Receipt is a recorder's confirmation of a proposal.
type Receipt struct {
	Slot     Slot
	Hash     ValueHash
	NodeID   NodeID
	Accepted bool
}

func EncodeLeaderSchedule(members []NodeID) ([]byte, error) {
	payload, err := json.Marshal(members)
	if err != nil {
		return nil, err
	}
	return append(append([]byte(nil), leaderScheduleMagic...), payload...), nil
}

func DecodeLeaderSchedule(value []byte) ([]NodeID, bool, error) {
	if !bytes.HasPrefix(value, leaderScheduleMagic) {
		return nil, false, nil
	}
	var members []NodeID
	if err := json.Unmarshal(value[len(leaderScheduleMagic):], &members); err != nil {
		return nil, true, err
	}
	if len(members) == 0 {
		return nil, true, fmt.Errorf("empty leader schedule")
	}
	return members, true, nil
}

func EncodeReadBarrier(nonce [ReadBarrierNonceSize]byte) []byte {
	return append(append([]byte(nil), readBarrierMagic...), nonce[:]...)
}

func DecodeReadBarrier(value []byte) (bool, error) {
	if !bytes.HasPrefix(value, readBarrierMagic) {
		return false, nil
	}
	if len(value) != len(readBarrierMagic)+ReadBarrierNonceSize {
		return true, fmt.Errorf("invalid read barrier nonce")
	}
	return true, nil
}

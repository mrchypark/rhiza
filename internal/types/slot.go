package types

import "github.com/mrchypark/rhiza/pkg/quepaxa"

// Slot is a monotonically increasing consensus position.
// Each slot can hold exactly one decided value.
type Slot = quepaxa.Slot

// NodeID uniquely identifies a node in the cluster.
type NodeID = quepaxa.NodeID

// ClusterID uniquely identifies a cluster.
type ClusterID string

// ValueHash is the SHA-256 hash of a proposed value.
type ValueHash = quepaxa.ValueHash

// SlotRange represents a contiguous range of slots [Start, End).
type SlotRange struct {
	Start Slot
	End   Slot // exclusive
}

func (r SlotRange) Len() uint64 {
	return uint64(r.End) - uint64(r.Start)
}

func (r SlotRange) Contains(s Slot) bool {
	return s >= r.Start && s < r.End
}

// Package fencing implements the cooperative process fencing protocol.
package fencing

import (
	"context"
	"errors"
)

const ProtocolVersion = 1

var (
	ErrPending     = errors.New("fencing: evidence pending")
	ErrClosed      = errors.New("fencing: incarnation terminally fenced")
	ErrUnavailable = errors.New("fencing: authority temporarily unavailable")
	ErrNotFound    = errors.New("fencing: immutable peer-closure request not found")
)

// Identity binds one supervisor launch; WALIdentity is assigned once, before writers start.
type Identity struct {
	SourceClusterID string `json:"sourceClusterID"`
	NodeID          string `json:"nodeID"`
	PodUID          string `json:"podUID"`
	RunID           string `json:"runID"`
	WALIdentity     string `json:"walIdentity"`
}

type StopRequest struct {
	OperationID string   `json:"operationID"`
	RequestHash string   `json:"requestHash"`
	Identity    Identity `json:"identity"`
}

// PeerClosureRequest asks the authority to close one other stable voter slot
// for an exact source membership round. The caller identity is carried
// separately in AuthorityMessage and is never part of the round operation ID.
type PeerClosureRequest struct {
	ExpectedConfigID  uint64  `json:"expectedConfigID"`
	ExpectedAbortSlot *uint64 `json:"expectedAbortSlot"`
	TargetNodeID      string  `json:"targetNodeID"`
	TargetWALIdentity string  `json:"targetWALIdentity"`
}

// Validate requires an explicitly supplied abort slot; a pointer to zero is a
// valid round value, while omission and JSON null are not.
func (r PeerClosureRequest) Validate(caller Identity) error {
	if caller.SourceClusterID == "" || caller.NodeID == "" || caller.PodUID == "" || caller.RunID == "" || caller.WALIdentity == "" ||
		r.ExpectedConfigID == 0 || r.ExpectedAbortSlot == nil || r.TargetNodeID == "" || r.TargetNodeID == caller.NodeID || r.TargetWALIdentity == "" {
		return errors.New("fencing: complete bound caller and target round are required")
	}
	return nil
}

// PeerClosureTarget is a target incarnation captured by the authority from its
// durable ledger and current Pod inventory. Empty runtime fields keep
// independent evidence pending; they are never inferred from a replacement.
type PeerClosureTarget struct {
	NodeID      string `json:"nodeID"`
	Pod         string `json:"pod"`
	PodUID      string `json:"podUID"`
	WALIdentity string `json:"walIdentity"`
	NodeName    string `json:"nodeName,omitempty"`
	NodeUID     string `json:"nodeUID,omitempty"`
	ContainerID string `json:"containerID,omitempty"`
	ImageID     string `json:"imageID,omitempty"`
}

// PeerClosureReceipt is an authority-validated projection of the immutable
// RhizaFence proof. It is returned only after exact request and live-binding
// verification succeeds.
type PeerClosureReceipt struct {
	OperationID         string              `json:"operationID"`
	SourceClusterID     string              `json:"sourceClusterID"`
	BindingUID          string              `json:"bindingUID"`
	StatefulSetUID      string              `json:"statefulSetUID"`
	RequestHash         string              `json:"requestHash"`
	ExpectedConfigID    uint64              `json:"expectedConfigID"`
	ExpectedAbortSlot   *uint64             `json:"expectedAbortSlot"`
	TargetNodeID        string              `json:"targetNodeID"`
	TargetWALIdentity   string              `json:"targetWALIdentity"`
	Targets             []PeerClosureTarget `json:"targets"`
	ProofID             string              `json:"proofID"`
	ProcessesTerminated bool                `json:"processesTerminated"`
	RecreationBlocked   bool                `json:"recreationBlocked"`
	StorageQuiesced     bool                `json:"storageQuiesced"`
}

// PeerClosureAuthority is deliberately separate from Authority: only the new
// peer-closure action requires survivor authorization; normal register/bind and
// termination actions retain their existing caller contract.
type PeerClosureAuthority interface {
	RequestPeerClosure(context.Context, Identity, PeerClosureRequest) (PeerClosureReceipt, error)
	ReadPeerClosure(context.Context, Identity, PeerClosureRequest) (PeerClosureReceipt, error)
}

// DrainReceipt is engine evidence, not evidence that the application process exited.
// Any unknown mutation keeps the incarnation ineligible even after SDK retries succeed.
type DrainReceipt struct {
	Request          StopRequest `json:"request"`
	WritersJoined    bool        `json:"writersJoined"`
	PendingMutations uint64      `json:"pendingMutations"`
	UnknownMutations uint64      `json:"unknownMutations"`
}

func (r DrainReceipt) Clean() bool {
	return r.WritersJoined && r.PendingMutations == 0 && r.UnknownMutations == 0
}

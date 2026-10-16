// Package fencing implements the cooperative process fencing protocol.
package fencing

import "errors"

const ProtocolVersion = 1

var (
	ErrPending     = errors.New("fencing: evidence pending")
	ErrClosed      = errors.New("fencing: incarnation terminally fenced")
	ErrUnavailable = errors.New("fencing: authority temporarily unavailable")
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

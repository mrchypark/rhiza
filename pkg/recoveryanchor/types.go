package recoveryanchor

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/mrchypark/rhiza/pkg/recovery"
)

const Version1 = 1

var (
	ErrInvalidRequest   = errors.New("recovery anchor: invalid request")
	ErrReceiptMismatch  = errors.New("recovery anchor: receipt mismatch")
	ErrNoTransition     = errors.New("recovery anchor: no pending transition")
	ErrAlreadyCommitted = errors.New("recovery anchor: transition already committed")
)

type requestError struct{ err error }

func (e requestError) Error() string { return e.err.Error() }
func (e requestError) Unwrap() error { return ErrInvalidRequest }

func invalidRequest(err error) error { return requestError{err: err} }

type Binding struct {
	ClusterID string `json:"cluster_id"`
	StorageID string `json:"storage_id"`
}

type Request struct {
	AnchorID         string                    `json:"anchor_id"`
	OperationID      string                    `json:"operation_id"`
	StatefulSetUID   string                    `json:"stateful_set_uid"`
	SourceClusterID  string                    `json:"source_cluster_id"`
	TargetClusterID  string                    `json:"target_cluster_id"`
	SourcePrefix     string                    `json:"source_prefix"`
	TargetPrefix     string                    `json:"target_prefix"`
	SourceMembership string                    `json:"source_membership"`
	FenceHash        string                    `json:"fence_hash"`
	TargetAnchorHash string                    `json:"target_anchor_hash"`
	Fork             recovery.ForkResult       `json:"fork"`
	TargetMembership recovery.MembershipRecord `json:"target_membership"`
}

type Record struct {
	Version        int         `json:"version"`
	EvidenceFormat string      `json:"evidence_format"`
	Binding        Binding     `json:"binding"`
	Evidence       []byte      `json:"evidence"`
	PendingWrite   bool        `json:"pending_write"`
	Generation     uint64      `json:"generation"`
	Transition     *Transition `json:"transition"`
	LastTransition *Receipt    `json:"last_transition"`
}

type Transition struct {
	Request    Request `json:"request"`
	Source     Binding `json:"source"`
	Generation uint64  `json:"generation"`
	Evidence   []byte  `json:"evidence"`
}

type Receipt struct {
	AnchorID    string  `json:"anchor_id"`
	OperationID string  `json:"operation_id"`
	RequestHash string  `json:"request_hash"`
	Target      Binding `json:"target"`
	Generation  uint64  `json:"generation"`
}

type Backend interface {
	Read(ctx context.Context, id string) (Record, string, error)
	CAS(ctx context.Context, id string, version string, record Record) error
}

func RequestHash(r Request) (string, error) {
	data, err := json.Marshal(r)
	if err != nil {
		return "", fmt.Errorf("marshal request: %w", err)
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:]), nil
}

package network

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"time"

	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

type MembershipFence struct {
	NodeID      quepaxa.NodeID `json:"node_id"`
	WALIdentity string         `json:"wal_identity"`
	WorkloadUID string         `json:"workload_uid"`
	Confirmed   bool           `json:"confirmed"`
	Evidence    string         `json:"evidence"`
}

// MembershipChange identifies one immutable operation against one configuration.
// Fence attests external exclusion of the removed incarnation, including its
// storage writers and replacement scheduling; deleting a Pod alone is not a fence.
type MembershipChange struct {
	OperationID       string           `json:"operation_id"`
	ClusterID         string           `json:"cluster_id"`
	ExpectedConfigID  uint             `json:"expected_config_id"`
	ExpectedAbortSlot quepaxa.Slot     `json:"expected_abort_slot,omitempty"`
	Remove            quepaxa.NodeID   `json:"remove,omitempty"`
	Add               *quepaxa.Member  `json:"add,omitempty"`
	Fence             *MembershipFence `json:"fence,omitempty"`
}

func (s *Server) SetMembershipChange(change func(context.Context, MembershipChange) error) {
	s.membershipChange = change
}

func (s *Server) SetMembershipAbort(abort func(context.Context, MembershipChange) error) {
	s.membershipAbort = abort
}

// ChangeMembership joins the server lifecycle so shutdown cannot close its WAL
// while a management operation is still using it.
func (s *Server) ChangeMembership(ctx context.Context, change MembershipChange) error {
	return s.runMembershipChange(ctx, change, false)
}

// AbortMembership terminates a pending addition under its original voter quorum.
// OperationID, ClusterID, ExpectedConfigID and ExpectedAbortSlot suffice; callers
// need not retain the failed learner's credentials to abort its recorded operation.
func (s *Server) AbortMembership(ctx context.Context, change MembershipChange) error {
	return s.runMembershipChange(ctx, change, true)
}

func (s *Server) runMembershipChange(ctx context.Context, change MembershipChange, abort bool) error {
	s.proposeMu.Lock()
	callback := s.membershipChange
	if abort {
		callback = s.membershipAbort
	}
	if s.closing || s.quiescing || callback == nil {
		s.proposeMu.Unlock()
		return ErrNotReady
	}
	s.proposalWG.Add(1)
	s.proposeMu.Unlock()
	defer s.proposalWG.Done()
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	stop := context.AfterFunc(s.proposalCtx, cancel)
	defer stop()
	return callback(ctx, change)
}

func (s *Server) handleMembershipChange(w http.ResponseWriter, r *http.Request) {
	s.handleMembershipMutation(w, r, false)
}

func (s *Server) handleMembershipAbort(w http.ResponseWriter, r *http.Request) {
	s.handleMembershipMutation(w, r, true)
}

func (s *Server) handleMembershipMutation(w http.ResponseWriter, r *http.Request, abort bool) {
	if r.Method != http.MethodPost {
		writeMethodNotAllowed(w, http.MethodPost)
		return
	}
	if s.membershipToken == "" || !matchesRecoveryBearer(r.Header.Get("Authorization"), s.membershipToken) {
		writeHTTPError(w, http.StatusForbidden, "forbidden", "membership management is not authorized")
		return
	}
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 64<<10))
	decoder.DisallowUnknownFields()
	var change MembershipChange
	if err := decoder.Decode(&change); err != nil {
		writeHTTPError(w, http.StatusBadRequest, "invalid_request", "invalid membership request")
		return
	}
	if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
		writeHTTPError(w, http.StatusBadRequest, "invalid_request", "unexpected trailing request data")
		return
	}
	if err := s.runMembershipChange(r.Context(), change, abort); err != nil {
		writeAPIError(w, err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(struct {
		Status string `json:"status"`
	}{"completed"})
}

// MembershipStatus contains no peer credentials. WALIdentity identifies this
// process's persistent incarnation; it is not evidence that another is fenced.
type MembershipStatus struct {
	NodeID      quepaxa.NodeID   `json:"node_id"`
	ClusterID   string           `json:"cluster_id"`
	ConfigID    uint             `json:"config_id"`
	Voters      []quepaxa.NodeID `json:"voters"`
	WALIdentity string           `json:"wal_identity"`
	Voting      bool             `json:"voting"`
	Pending     bool             `json:"pending"`
	AbortSlot   quepaxa.Slot     `json:"abort_slot,omitempty"`
}

// SetMembershipToken configures management authentication before serving.
func (s *Server) SetMembershipToken(token string) { s.membershipToken = token }

func (s *Server) handleMembershipStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeMethodNotAllowed(w, http.MethodGet)
		return
	}
	if s.membershipToken == "" || !matchesRecoveryBearer(r.Header.Get("Authorization"), s.membershipToken) {
		writeHTTPError(w, http.StatusForbidden, "forbidden", "membership management is not authorized")
		return
	}
	status, err := s.MembershipStatus()
	if err != nil {
		writeAPIError(w, err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(status)
}

// MembershipStatus is an observational snapshot, never a fencing authorization.
func (s *Server) MembershipStatus() (MembershipStatus, error) {
	s.proposeMu.Lock()
	defer s.proposeMu.Unlock()
	if s.closing || s.core == nil || s.membershipToken == "" {
		return MembershipStatus{}, ErrNotReady
	}
	cluster := s.core.CurrentCluster()
	_, pending := s.core.PendingReconfiguration()
	status := MembershipStatus{NodeID: s.core.NodeID(), ClusterID: string(s.cluster), ConfigID: cluster.ConfigID, WALIdentity: s.core.WALIdentity(), Voting: s.core.IsVoter(), Pending: pending}
	status.AbortSlot, _, _ = s.core.LastReconfigurationAbort()
	for _, member := range cluster.Members {
		status.Voters = append(status.Voters, member.ID)
	}
	return status, nil
}

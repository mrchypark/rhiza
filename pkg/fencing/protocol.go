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
	// PodName is the authenticated physical Kubernetes workload name. It is
	// omitted for legacy/static registrations, where NodeID remains the Pod name.
	PodName      string `json:"podName,omitempty"`
	PodUID       string `json:"podUID"`
	RunID        string `json:"runID"`
	WALIdentity  string `json:"walIdentity"`
	AssignmentID string `json:"assignmentID,omitempty"`
}

type StartupRole string

const (
	StartupRoleVoterActive          StartupRole = "voter-active"
	StartupRoleLearnerPendingRemove StartupRole = "learner-pending-remove"
)

func (r StartupRole) Valid() bool {
	return r == StartupRoleVoterActive || r == StartupRoleLearnerPendingRemove
}

// StartupGrant is returned only by the authenticated authority registration.
// The child cannot request or choose its role. Older authorities that return an
// empty response remain static-voter compatible through the supervisor's
// legacy registration path.
type StartupGrant struct {
	Identity Identity    `json:"identity"`
	Role     StartupRole `json:"role"`
	// Environment contains only the bounded, fencer-derived overrides needed
	// to start an assigned same-ordinal learner. It is returned over the
	// authenticated TLS supervisor channel and is never persisted or logged.
	Environment map[string]string `json:"environment,omitempty"`
}

func (g StartupGrant) Validate(request Identity) error {
	physical := request.PodName
	if physical == "" {
		physical = request.NodeID
	}
	if !g.Role.Valid() || g.Identity.SourceClusterID == "" || g.Identity.NodeID == "" || g.Identity.PodUID == "" || g.Identity.RunID == "" || g.Identity.SourceClusterID != request.SourceClusterID || g.Identity.PodUID != request.PodUID || g.Identity.RunID != request.RunID || g.Identity.WALIdentity != "" || physical == "" {
		return errors.New("fencing: startup grant identity differs")
	}
	assignedPhysical := g.Identity.PodName
	if assignedPhysical == "" {
		assignedPhysical = g.Identity.NodeID
	}
	if assignedPhysical != physical {
		return errors.New("fencing: startup grant physical Pod differs")
	}
	if g.Identity.NodeID != physical {
		if g.Identity.AssignmentID == "" || !mutationDigestValid(g.Identity.AssignmentID) {
			return errors.New("fencing: logical identity requires an authority-issued assignment")
		}
		if g.Environment["RHIZA_NODE_ID"] != g.Identity.NodeID || g.Environment["RHIZA_PEER_TOKEN"] == "" || g.Environment["RHIZA_PEER_TOKENS"] != "" ||
			(g.Role == StartupRoleLearnerPendingRemove && g.Environment["RHIZA_LEARNER"] == "") ||
			(g.Role == StartupRoleVoterActive && (g.Environment["RHIZA_LEARNER"] != "" || g.Environment["RHIZA_CLUSTER_MEMBERS"] == "")) {
			return errors.New("fencing: learner assignment configuration is incomplete")
		}
	} else if g.Identity.AssignmentID != "" {
		return errors.New("fencing: physical identity cannot carry a logical assignment")
	}
	environmentBytes := 0
	for key, value := range g.Environment {
		if key != "RHIZA_NODE_ID" && key != "RHIZA_LEARNER" && key != "RHIZA_PEER_TOKEN" && key != "RHIZA_PEER_TOKENS" && key != "RHIZA_CLUSTER_MEMBERS" {
			return errors.New("fencing: startup grant contains an unsupported environment override")
		}
		environmentBytes += len(key) + len(value)
		if len(value) > 16<<10 || environmentBytes > 32<<10 {
			return errors.New("fencing: startup grant environment exceeds capacity")
		}
	}
	if len(g.Environment) != 0 && (g.Identity.AssignmentID == "" || g.Identity.NodeID == physical || (g.Role != StartupRoleLearnerPendingRemove && g.Role != StartupRoleVoterActive)) {
		return errors.New("fencing: startup environment is restricted to assigned identities")
	}
	return nil
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

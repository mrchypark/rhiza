package operator

import (
	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

type Resource struct {
	APIVersion string         `json:"apiVersion"`
	Kind       string         `json:"kind"`
	Metadata   map[string]any `json:"metadata"`
	Spec       Spec           `json:"spec"`
	Status     Status         `json:"status,omitempty"`
}

type Spec struct {
	StatefulSet     string `json:"statefulSet"`
	Container       string `json:"container,omitempty"`
	SourceClusterID string `json:"sourceClusterID"`
	// Durability is the NEW generation's mode. The immutable source record,
	// not this desired setting, determines whether recovery can lose ACKs.
	Durability           string          `json:"durability"`
	RecoveryID           string          `json:"recoveryID,omitempty"`
	AllowDataLoss        bool            `json:"allowDataLoss,omitempty"`
	MaxArchiveAgeSeconds int64           `json:"maxArchiveAgeSeconds,omitempty"`
	Fence                Fence           `json:"fence,omitempty"`
	Membership           *MembershipSpec `json:"membership,omitempty"`
}

// MembershipSpec replaces one externally fenced voter through a separately
// provisioned learner. The controller never creates, deletes, or rolls either
// workload.
type MembershipSpec struct {
	OperationID string          `json:"operationID"`
	Remove      quepaxa.NodeID  `json:"remove"`
	Fence       MembershipFence `json:"fence"`
	// VoterPods names every currently reachable voter that may form quorum.
	// It deliberately does not discover Pods or infer an ID from a Pod name.
	VoterPods         []VoterPod `json:"voterPods"`
	RemovedPod        string     `json:"removedPod,omitempty"`
	AbortAddition     bool       `json:"abortAddition,omitempty"`
	ReplacementPod    string     `json:"replacementPod"`
	ReplacementSecret string     `json:"replacementSecret"`
}

type VoterPod struct {
	NodeID quepaxa.NodeID `json:"nodeID"`
	Pod    string         `json:"pod"`
}

type MembershipFence struct {
	NodeID      quepaxa.NodeID `json:"nodeID"`
	WALIdentity string         `json:"walIdentity"`
	WorkloadUID string         `json:"workloadUID"`
	Confirmed   bool           `json:"confirmed"`
	Evidence    string         `json:"evidence"`
}

type Fence struct {
	RecoveryID     string `json:"recoveryID"`
	ClusterID      string `json:"clusterID"`
	StatefulSetUID string `json:"statefulSetUID"`
	Confirmed      bool   `json:"confirmed"`
	Evidence       string `json:"evidence"`
}

type Status struct {
	Phase              string                        `json:"phase"`
	Message            string                        `json:"message"`
	ObservedGeneration int64                         `json:"observedGeneration"`
	Source             string                        `json:"source"`
	Target             string                        `json:"target,omitempty"`
	RecoveryID         string                        `json:"recoveryID,omitempty"`
	SpecHash           string                        `json:"specHash,omitempty"`
	StatefulSetUID     string                        `json:"statefulSetUID,omitempty"`
	SourceDurability   string                        `json:"sourceDurability,omitempty"`
	SourceMembership   string                        `json:"sourceMembership,omitempty"`
	ManifestHash       string                        `json:"manifestHash,omitempty"`
	RecoveredTip       uint64                        `json:"recoveredTip,omitempty"`
	SecretName         string                        `json:"secretName,omitempty"`
	Stage              string                        `json:"stage,omitempty"`
	ArchiveCapture     string                        `json:"archiveCapture,omitempty"`
	Peers              []network.VoterRecoveryStatus `json:"peers,omitempty"`
	Membership         *MembershipStatus             `json:"membership,omitempty"`
}

// MembershipStatus is the CR's durable side-effect journal. The remove request
// is retained directly; the add request is bound by hash to an immutable Secret
// without persisting its credential.
type MembershipStatus struct {
	Phase         string                    `json:"phase"`
	RemoveRequest *network.MembershipChange `json:"removeRequest,omitempty"`
	Add           *MembershipAddJournal     `json:"add,omitempty"`
}

// MembershipAddJournal intentionally excludes the learner token. It binds the
// exact request bytes to one immutable credential Secret instead.
type MembershipAddJournal struct {
	OperationID       string         `json:"operationID"`
	ExpectedConfigID  uint           `json:"expectedConfigID"`
	ExpectedAbortSlot quepaxa.Slot   `json:"expectedAbortSlot"`
	MemberID          quepaxa.NodeID `json:"memberID"`
	WALIdentity       string         `json:"walIdentity"`
	RequestHash       string         `json:"requestHash"`
	SecretUID         string         `json:"secretUID"`
	SecretResourceVer string         `json:"secretResourceVersion"`
}

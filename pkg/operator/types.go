package operator

import "github.com/mrchypark/rhiza/pkg/network"

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
	Durability           string `json:"durability"`
	RecoveryID           string `json:"recoveryID,omitempty"`
	AllowDataLoss        bool   `json:"allowDataLoss,omitempty"`
	MaxArchiveAgeSeconds int64  `json:"maxArchiveAgeSeconds,omitempty"`
	Fence                Fence  `json:"fence,omitempty"`
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
}

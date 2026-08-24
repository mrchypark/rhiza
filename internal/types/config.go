package types

import (
	"time"

	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

// NodeConfig describes a single node in the cluster.
type NodeConfig = quepaxa.Member

// ClusterConfig describes the current cluster membership.
type ClusterConfig = quepaxa.Cluster

// NodeRole is the role of a node in the cluster.
type NodeRole int

const (
	RoleLearner NodeRole = iota
	RoleVoter
)

// Profile is the runtime execution profile.
type Profile string

const (
	ProfileSQL   Profile = "sql"
	ProfileGraph Profile = "graph"
	ProfileKV    Profile = "kv"
)

// ExecutionConfig holds runtime configuration.
type ExecutionConfig struct {
	ClusterID   ClusterID    `json:"cluster_id"`
	NodeID      NodeID       `json:"node_id"`
	Profile     Profile      `json:"profile"`
	DataDir     string       `json:"data_dir"`
	BindAddr    string       `json:"bind_addr"`
	PeerAddr    string       `json:"peer_addr"`
	LogAddr     string       `json:"log_addr"`
	AdminToken  string       `json:"admin_token"`
	ClientToken string       `json:"client_token"`
	Members     []NodeConfig `json:"members"`

	// Object store configuration
	ObjStoreEndpoint string `json:"objstore_endpoint"`
	ObjStoreBucket   string `json:"objstore_bucket"`

	// Timing
	CheckpointInterval time.Duration `json:"checkpoint_interval"`
	ReadTimeout        time.Duration `json:"read_timeout"`
	HedgeDelay         time.Duration `json:"hedge_delay"`
}

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

type ObjectStoreDurability string

const (
	ProfileSQL   Profile = "sql"
	ProfileGraph Profile = "graph"

	ObjectStoreDurabilityAsync     ObjectStoreDurability = "async"
	ObjectStoreDurabilityBeforeAck ObjectStoreDurability = "before-ack"
)

// ExecutionConfig holds runtime configuration.
type ExecutionConfig struct {
	ClusterID  ClusterID    `json:"cluster_id"`
	NodeID     NodeID       `json:"node_id"`
	Profile    Profile      `json:"profile"`
	DataDir    string       `json:"data_dir"`
	BindAddr   string       `json:"bind_addr"`
	PeerAddr   string       `json:"peer_addr"`
	AdminToken string       `json:"admin_token"`
	Members    []NodeConfig `json:"members"`

	// Object store configuration
	ObjStoreEndpoint      string                `json:"objstore_endpoint"`
	ObjStoreBucket        string                `json:"objstore_bucket"`
	ObjStoreProvider      string                `json:"objstore_provider"`
	ObjStoreDir           string                `json:"objstore_dir"`
	ObjStorePrefix        string                `json:"objstore_prefix"`
	ObjStoreRegion        string                `json:"objstore_region"`
	ObjStoreInsecure      bool                  `json:"objstore_insecure"`
	ObjStoreRetries       int                   `json:"objstore_retries"`
	ObjStoreAccessKey     string                `json:"objstore_access_key"`
	ObjStoreSecretKey     string                `json:"objstore_secret_key"`
	ObjStoreSessionToken  string                `json:"objstore_session_token"`
	ObjStoreDurability    ObjectStoreDurability `json:"objstore_durability"`
	ObjStoreSyncInterval  time.Duration         `json:"objstore_sync_interval"`
	ObjStoreGCInterval    time.Duration         `json:"objstore_gc_interval"`
	ObjStoreGCGracePeriod time.Duration         `json:"objstore_gc_grace_period"`

	// Timing
	CheckpointInterval  time.Duration `json:"checkpoint_interval"`
	CheckpointTailBytes int64         `json:"checkpoint_tail_bytes"`
	HedgeDelay          time.Duration `json:"hedge_delay"`
}

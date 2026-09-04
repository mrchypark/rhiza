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

type ObjectStoreDurability string

const (
	ObjectStoreDurabilityAsync     ObjectStoreDurability = "async"
	ObjectStoreDurabilityBeforeAck ObjectStoreDurability = "before-ack"
)

// GraphNodePropertyIndex is node-local derived state rebuilt when a materializer is replaced.
type GraphNodePropertyIndex struct {
	Label    string `json:"label"`
	Property string `json:"property"`
}

// ExecutionConfig holds runtime configuration.
type ExecutionConfig struct {
	ClusterID                     ClusterID                `json:"cluster_id"`
	NodeID                        NodeID                   `json:"node_id"`
	DataDir                       string                   `json:"data_dir"`
	BindAddr                      string                   `json:"bind_addr"`
	PeerAddr                      string                   `json:"peer_addr"`
	AdminToken                    string                   `json:"admin_token"`
	Members                       []NodeConfig             `json:"members"`
	LocalGraphNodePropertyIndexes []GraphNodePropertyIndex `json:"local_graph_node_property_indexes,omitempty"`

	// Object store configuration
	ObjStoreEndpoint               string                `json:"objstore_endpoint"`
	ObjStoreBucket                 string                `json:"objstore_bucket"`
	ObjStoreProvider               string                `json:"objstore_provider"`
	ObjStoreDir                    string                `json:"objstore_dir"`
	ObjStorePrefix                 string                `json:"objstore_prefix"`
	ObjStoreRegion                 string                `json:"objstore_region"`
	ObjStoreInsecure               bool                  `json:"objstore_insecure"`
	ObjStoreRetries                int                   `json:"objstore_retries"`
	ObjStoreAccessKey              string                `json:"objstore_access_key"`
	ObjStoreSecretKey              string                `json:"objstore_secret_key"`
	ObjStoreSessionToken           string                `json:"objstore_session_token"`
	ObjStoreServiceAccount         string                `json:"objstore_service_account"`
	ObjStoreAzureTenantID          string                `json:"objstore_azure_tenant_id"`
	ObjStoreAzureClientID          string                `json:"objstore_azure_client_id"`
	ObjStoreAzureClientSecret      string                `json:"objstore_azure_client_secret"`
	ObjStoreAzureStorageAccount    string                `json:"objstore_azure_storage_account"`
	ObjStoreAzureStorageAccountKey string                `json:"objstore_azure_storage_account_key"`
	ObjStoreAzureConnectionString  string                `json:"objstore_azure_connection_string"`
	ObjStoreAzureUserAssignedID    string                `json:"objstore_azure_user_assigned_id"`
	ObjStoreDurability             ObjectStoreDurability `json:"objstore_durability"`
	ObjStoreSyncInterval           time.Duration         `json:"objstore_sync_interval"`
	ObjStoreBatchDelay             time.Duration         `json:"objstore_batch_delay"`
	ObjStoreGCInterval             time.Duration         `json:"objstore_gc_interval"`
	ObjStoreGCGracePeriod          time.Duration         `json:"objstore_gc_grace_period"`

	// Timing
	CheckpointInterval  time.Duration `json:"checkpoint_interval"`
	CheckpointTailBytes int64         `json:"checkpoint_tail_bytes"`
	MaxWALBytes         int64         `json:"max_wal_bytes"`
}

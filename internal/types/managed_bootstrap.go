package types

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/mrchypark/rhiza/internal/sqlpolicy"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

// ManagedClusterID derives a generation identity independently of member keys.
func ManagedClusterID(crUID string, generation int64) (ClusterID, error) {
	if !validManagedUID(crUID) || generation < 0 {
		return "", fmt.Errorf("invalid managed cluster identity")
	}
	hash := sha256.Sum256([]byte("rhiza-managed-cluster-v1\x00" + crUID + "\x00" + strconv.FormatInt(generation, 10)))
	return ClusterID(fmt.Sprintf("rhiza-c-%x", hash)), nil
}

// ManagedNodeID derives a fresh member identity from the actual Pod UID.
func ManagedNodeID(podUID string) (NodeID, error) {
	if !validManagedUID(podUID) {
		return "", fmt.Errorf("invalid managed pod identity")
	}
	hash := sha256.Sum256([]byte("rhiza-managed-node-v1\x00" + podUID))
	return NodeID(fmt.Sprintf("rhiza-n-%x", hash)), nil
}

func validManagedUID(uid string) bool {
	return len(uid) > 0 && len(uid) <= 256 && utf8.ValidString(uid) && strings.TrimSpace(uid) == uid && !strings.ContainsRune(uid, '\x00')
}

// ManagedBootstrapPlanDigest matches the managed Node's common bootstrap
// digest; member-specific WAL identities do not belong to the common plan.
func ManagedBootstrapPlanDigest(clusterID string, members []quepaxa.Member, physicalBinding, evidenceFormat string, durability ObjectStoreDurability, reconfiguration bool) (string, error) {
	return ManagedBootstrapPlanDigestForProtocol(clusterID, members, physicalBinding, evidenceFormat, durability, reconfiguration, "")
}

// ManagedApplicationPolicy selects only from the caller's trusted protocol.
// Managed startup has no configurable retention window.
func ManagedApplicationPolicy(protocol string) (sqlpolicy.MaterializationPolicy, uint64, error) {
	switch protocol {
	case "":
		return sqlpolicy.LegacyMaterializationPolicy, DefaultIdempotencyWindowSlots, nil
	case ManagedApplicationProtocolSQLKV:
		return sqlpolicy.ManagedSQLKVMaterializationPolicy, DefaultIdempotencyWindowSlots, nil
	case sqlpolicy.ManagedRequestApplicationProtocol:
		return sqlpolicy.ManagedRequestMaterializationPolicy, DefaultIdempotencyWindowSlots, nil
	default:
		return 0, 0, fmt.Errorf("unsupported managed application protocol")
	}
}

func ManagedBootstrapPlanDigestForProtocol(clusterID string, members []quepaxa.Member, physicalBinding, evidenceFormat string, durability ObjectStoreDurability, reconfiguration bool, protocol string) (string, error) {
	_, window, err := ManagedApplicationPolicy(protocol)
	if err != nil {
		return "", err
	}
	if protocol == "" {
		window = 0 // Preserve the original serialized digest byte for byte.
	}
	if clusterID == "" || physicalBinding == "" || evidenceFormat == "" {
		return "", fmt.Errorf("invalid managed bootstrap binding")
	}
	members = slices.Clone(members)
	ids, keys, endpoints := map[quepaxa.NodeID]bool{}, map[quepaxa.PublicKey]bool{}, map[string]bool{}
	for i := range members {
		m := members[i]
		if m.ID == "" || m.PublicKey == (quepaxa.PublicKey{}) || m.PeerURL == "" || ids[m.ID] || keys[m.PublicKey] || endpoints[m.PeerURL] {
			return "", fmt.Errorf("invalid or duplicate bootstrap member")
		}
		ids[m.ID], keys[m.PublicKey], endpoints[m.PeerURL] = true, true, true
		members[i].WALIdentity = ""
	}
	slices.SortFunc(members, func(a, b quepaxa.Member) int { return strings.Compare(string(a.ID), string(b.ID)) })
	if durability == "" {
		durability = ObjectStoreDurabilityBeforeAck
	}
	data, err := json.Marshal(struct {
		Version             int
		ClusterID           string
		Members             []quepaxa.Member
		PhysicalBinding     string
		EvidenceFormat      string
		Durability          ObjectStoreDurability
		Reconfiguration     bool
		ApplicationProtocol string `json:",omitempty"`
		IdempotencyWindow   uint64 `json:",omitempty"`
	}{1, clusterID, members, physicalBinding, evidenceFormat, durability, reconfiguration, protocol, window})
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%x", sha256.Sum256(data)), nil
}

// ManagedMemberConfigDigest is shared by pure bootstrap binding checks.
func ManagedMemberConfigDigest(config ExecutionConfig, protocol string) (string, error) {
	_, window, err := ManagedApplicationPolicy(protocol)
	if err != nil {
		return "", err
	}
	if protocol == "" {
		window = 0
	}
	data, err := json.Marshal(struct {
		Cluster             string
		Node                string
		Members             []quepaxa.Member
		Learner             *quepaxa.Member
		Durability          string
		Reconfiguration     bool
		ApplicationProtocol string `json:",omitempty"`
		IdempotencyWindow   uint64 `json:",omitempty"`
	}{string(config.ClusterID), string(config.NodeID), config.Members, config.Learner, string(config.ObjStoreDurability), config.EnableReconfiguration, protocol, window})
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%x", sha256.Sum256(data)), nil
}

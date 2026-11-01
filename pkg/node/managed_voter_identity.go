package node

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"path"
	"slices"

	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

// ManagedVoterWALIdentity reads an existing current voter's immutable
// registration. It never registers an identity or opens/reconstructs a WAL.
func (n *Node) ManagedVoterWALIdentity(ctx context.Context, configID uint, abortSlot quepaxa.Slot, member quepaxa.Member) (string, error) {
	if n == nil {
		return "", ErrVoterStateLost
	}
	n.shutdownMu.Lock()
	defer n.shutdownMu.Unlock()
	if err := n.managedRepairReady(); err != nil {
		return "", err
	}
	check := func() error {
		current := n.core.CurrentCluster()
		abort, _, _ := n.core.LastReconfigurationAbort()
		_, pending := n.core.PendingReconfiguration()
		if current.ConfigID != configID || abort != abortSlot || pending || member.PublicKey == (quepaxa.PublicKey{}) || !slices.Contains(current.Members, member) {
			return fmt.Errorf("current voter membership revision changed")
		}
		return nil
	}
	if err := check(); err != nil {
		return "", err
	}
	source, err := types.ManagedStorageBinding(*n.config)
	if err != nil || source != n.managedStartup.Identity.PhysicalBinding {
		return "", fmt.Errorf("managed voter physical binding changed")
	}
	key := path.Join(n.config.ObjStorePrefix, string(n.config.ClusterID), "voters", fmt.Sprintf("%x.json", sha256.Sum256([]byte(member.ID))))
	data, err := readVoterRegistration(ctx, n.bucket, key)
	if err != nil {
		return "", err
	}
	var got voterIdentity
	if len(data) == 0 || json.Unmarshal(data, &got) != nil {
		return "", ErrVoterStateLost
	}
	nonce, err := hex.DecodeString(got.Nonce)
	if err != nil || len(nonce) != 32 || !got.Registered {
		return "", ErrVoterStateLost
	}
	cfg := *n.config
	cfg.NodeID, cfg.Learner = member.ID, nil
	if got.LearnerPublicKey != "" {
		if got.LearnerPublicKey != fmt.Sprintf("%x", member.PublicKey[:]) {
			return "", ErrVoterStateLost
		}
		cfg.Learner = &member
	} else if !slices.ContainsFunc(cfg.Members, func(original quepaxa.Member) bool {
		return original.ID == member.ID && original.PublicKey == member.PublicKey
	}) {
		return "", fmt.Errorf("bootstrap voter key differs from current certified member")
	}
	want := voterIdentityFor(&cfg)
	want.Nonce, want.Registered = got.Nonce, true
	if got != want || member.WALIdentity != "" && member.WALIdentity != got.Nonce {
		return "", ErrVoterStateLost
	}
	if err := check(); err != nil {
		return "", err
	}
	return got.Nonce, nil
}

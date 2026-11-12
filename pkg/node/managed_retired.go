package node

import (
	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

// ManagedVoterRetired reports the runtime's certified retired-identity set.
// It is safe before admission, but must not be called while the Node shutdown
// lock is held because it is only a non-authoritative precondition check.
func (n *Node) ManagedVoterRetired(id quepaxa.NodeID) (bool, error) {
	if n == nil || n.core == nil || !n.opened.Load() || id == "" {
		return false, network.ErrNotReady
	}
	return n.core.IsRetired(id), nil
}

package quepaxa

import (
	"errors"
	"fmt"

	"github.com/mrchypark/rhiza/pkg/qlog"
)

var ErrInvalidConfig = errors.New("invalid QuePaxa config")

// Config contains the dependencies and fixed membership of one Core.
type Config struct {
	NodeID  NodeID
	Cluster Cluster
	// WAL must be open for the Core's lifetime; the caller retains ownership.
	WAL       *qlog.WAL
	Transport Transport
}

// New validates and creates a QuePaxa core.
func New(config Config) (*Core, error) {
	if config.NodeID == "" {
		return nil, fmt.Errorf("%w: node ID is required", ErrInvalidConfig)
	}
	if config.WAL == nil {
		return nil, fmt.Errorf("%w: WAL is required", ErrInvalidConfig)
	}
	if len(config.Cluster.Members) == 0 {
		return nil, fmt.Errorf("%w: at least one member is required", ErrInvalidConfig)
	}
	seen := make(map[NodeID]struct{}, len(config.Cluster.Members))
	local := false
	for _, member := range config.Cluster.Members {
		if member.ID == "" {
			return nil, fmt.Errorf("%w: member ID is required", ErrInvalidConfig)
		}
		if _, duplicate := seen[member.ID]; duplicate {
			return nil, fmt.Errorf("%w: duplicate member %q", ErrInvalidConfig, member.ID)
		}
		seen[member.ID] = struct{}{}
		local = local || member.ID == config.NodeID
	}
	if !local {
		return nil, fmt.Errorf("%w: local node %q is not a member", ErrInvalidConfig, config.NodeID)
	}
	if len(config.Cluster.Members) > 1 && config.Transport == nil {
		return nil, fmt.Errorf("%w: transport is required for multiple members", ErrInvalidConfig)
	}
	cluster := config.Cluster
	cluster.Members = append([]Member(nil), config.Cluster.Members...)
	core := newCore(config.NodeID, &cluster, config.WAL, config.Transport)
	if err := core.recover(); err != nil {
		return nil, fmt.Errorf("recover QuePaxa: %w", err)
	}
	return core, nil
}

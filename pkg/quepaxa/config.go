package quepaxa

import (
	"context"
	"crypto/rand"
	"encoding/hex"
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
	// EnableReconfiguration opts into the bounded, in-band prototype. It is
	// deliberately off by default while checkpoint snapshots lack certified
	// membership state.
	EnableReconfiguration bool
	// ReconfigurationAdmission must approve every added voter after it has
	// caught up durably. A nil callback rejects additions.
	ReconfigurationAdmission func(context.Context, Cluster, Slot, [32]byte) error
}

// New validates and creates a QuePaxa core.
func New(config Config) (*Core, error) {
	return newCoreFromConfig(config, false, false)
}

// NewObserver creates a passive verifier whose node ID must not be a voter.
func NewObserver(config Config) (*Core, error) {
	return newCoreFromConfig(config, true, false)
}

// NewLearner creates a non-voting replica that can verify and durably catch
// up. It becomes a voter only after a certified target configuration names
// its previously absent ID.
func NewLearner(config Config) (*Core, error) { return newCoreFromConfig(config, false, true) }

func newCoreFromConfig(config Config, observer, learner bool) (*Core, error) {
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
	var localMember Member
	for _, member := range config.Cluster.Members {
		if member.ID == "" {
			return nil, fmt.Errorf("%w: member ID is required", ErrInvalidConfig)
		}
		if _, duplicate := seen[member.ID]; duplicate {
			return nil, fmt.Errorf("%w: duplicate member %q", ErrInvalidConfig, member.ID)
		}
		seen[member.ID] = struct{}{}
		local = local || member.ID == config.NodeID
		if member.ID == config.NodeID {
			localMember = member
		}
	}
	if !local && !observer && !learner {
		return nil, fmt.Errorf("%w: local node %q is not a member", ErrInvalidConfig, config.NodeID)
	}
	if local && observer {
		return nil, fmt.Errorf("%w: observer node %q is a member", ErrInvalidConfig, config.NodeID)
	}
	if local && learner {
		return nil, fmt.Errorf("%w: learner node %q is already a voter", ErrInvalidConfig, config.NodeID)
	}
	if len(config.Cluster.Members) > 1 && config.Transport == nil && !observer {
		return nil, fmt.Errorf("%w: transport is required for multiple members", ErrInvalidConfig)
	}
	cluster := config.Cluster
	cluster.Members = append([]Member(nil), config.Cluster.Members...)
	core := newCore(config.NodeID, &cluster, config.WAL, config.Transport)
	core.observer = observer
	core.learner = learner
	if learner {
		identity, ok := config.WAL.Identity()
		if !ok {
			identity = make([]byte, 32)
			if _, err := rand.Read(identity); err != nil {
				return nil, fmt.Errorf("create learner WAL identity: %w", err)
			}
			if err := config.WAL.BindIdentity(identity); err != nil {
				return nil, fmt.Errorf("bind learner WAL identity: %w", err)
			}
		}
		if len(identity) != 32 {
			return nil, fmt.Errorf("%w: learner WAL identity must be 32 bytes", ErrInvalidConfig)
		}
		core.walIdentity = hex.EncodeToString(identity)
	}
	if localMember.WALIdentity != "" {
		identity, ok := config.WAL.Identity()
		if !ok || len(identity) != 32 || hex.EncodeToString(identity) != localMember.WALIdentity {
			return nil, fmt.Errorf("%w: enrolled voter requires its original WAL identity", ErrInvalidConfig)
		}
		core.walIdentity = localMember.WALIdentity
	}
	core.reconfigEnabled = config.EnableReconfiguration
	core.reconfigAdmission = config.ReconfigurationAdmission
	core.configHistory = []configEpoch{{start: 1, cluster: cloneCluster(cluster)}}
	core.retiredIDs = make(map[NodeID]struct{})
	if err := core.recover(); err != nil {
		return nil, fmt.Errorf("recover QuePaxa: %w", err)
	}
	if config.EnableReconfiguration && !core.reconfigWAL {
		if err := config.WAL.Append(qlog.Entry{Type: qlog.EntryReceipt, Payload: append([]byte(nil), isrEntryV2Magic...)}); err != nil {
			return nil, err
		}
		if err := config.WAL.Sync(); err != nil {
			return nil, err
		}
		core.reconfigWAL = true
	}
	return core, nil
}

package node

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log"
	"os"
	"reflect"
	"time"

	"github.com/mrchypark/rhiza/internal/sqlpolicy"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/checkpoint"
	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/qlog"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

func isManagedAppendOnlyProtocol(protocol string) bool {
	return protocol == sqlpolicy.ManagedRequestAppendOnlyApplicationProtocol || protocol == sqlpolicy.ManagedGraphRequestAppendOnlyApplicationProtocol
}

func isManagedGraphApplicationProtocol(protocol string) bool {
	return protocol == sqlpolicy.ManagedGraphRequestApplicationProtocol || protocol == sqlpolicy.ManagedGraphRequestAppendOnlyApplicationProtocol
}

func (n *Node) managedApplicationProtocol() string {
	if n == nil || n.managedStartup == nil {
		return ""
	}
	return n.managedStartup.Identity.ApplicationProtocol
}

// Empty preserves the old Graph install journal protocol. The explicit V1
// profile must be carried into both target inspection and pair installation.
func (n *Node) managedGraphInstallProtocol() string {
	if n == nil || n.materializationPolicy() != sqlpolicy.ManagedGraphRequestMaterializationPolicy {
		return ""
	}
	if n.managedApplicationProtocol() == sqlpolicy.ManagedGraphRequestAppendOnlyApplicationProtocol {
		return sqlpolicy.ManagedGraphRequestAppendOnlyApplicationProtocol
	}
	// Inspection receives an explicit optional protocol, so preserve the old
	// Graph default as its exact value rather than passing an empty string.
	return sqlpolicy.ManagedGraphRequestApplicationProtocol
}

func (n *Node) managedAppendOnlyProfile() (*network.ManagedPeerProfile, error) {
	if n == nil || n.managedStartup == nil || !isManagedAppendOnlyProtocol(n.managedStartup.Identity.ApplicationProtocol) {
		return nil, nil
	}
	binding, err := n.managedVoterProfileFor(n.config)
	if err != nil {
		return nil, fmt.Errorf("managed append-only peer binding unavailable: %w", err)
	}
	if binding == nil {
		return nil, fmt.Errorf("managed append-only peer binding unavailable")
	}
	return &network.ManagedPeerProfile{ApplicationProtocol: binding.ApplicationProtocol, BootstrapPlanDigest: binding.BootstrapPlanDigest, Generation: binding.Generation}, nil
}

func (n *Node) managedAppendOnlyCoreConfig() (*quepaxa.ManagedAppendOnlyConfig, error) {
	profile, err := n.managedVoterProfileFor(n.config)
	if err != nil {
		return nil, fmt.Errorf("managed append-only voter binding unavailable: %w", err)
	}
	if profile == nil {
		return nil, fmt.Errorf("managed append-only voter binding unavailable")
	}
	identity, err := hex.DecodeString(n.managedStartupWALIdentity())
	if err != nil || len(identity) != 32 {
		return nil, fmt.Errorf("managed append-only WAL identity is invalid")
	}
	var walIdentity [32]byte
	copy(walIdentity[:], identity)
	return &quepaxa.ManagedAppendOnlyConfig{
		Binding: quepaxa.ManagedAppendOnlyBinding{
			ApplicationProtocol: profile.ApplicationProtocol,
			BootstrapPlanDigest: profile.BootstrapPlanDigest,
			MemberConfigDigest:  profile.MemberConfigDigest,
			Generation:          profile.Generation,
			WALIdentity:         walIdentity,
		},
		Verify: n.verifyManagedAppendOnlyCandidate,
	}, nil
}

func (n *Node) managedStartupWALIdentity() string {
	state, err := loadVoterIdentity(n.config)
	if err != nil || state.identity == nil || !state.identity.Registered {
		return ""
	}
	return state.identity.Nonce
}

func (n *Node) verifyManagedAppendOnlyCandidate(ctx context.Context, candidate quepaxa.ManagedCheckpointCandidate) error {
	if n == nil || n.checkpoints == nil || n.archive == nil || n.managedStartup == nil || !isManagedAppendOnlyProtocol(n.managedStartup.Identity.ApplicationProtocol) {
		return fmt.Errorf("managed append-only candidate verifier unavailable")
	}
	policy := n.materializationPolicy()
	root, err := n.checkpoints.OpenRoot(ctx, uint64(candidate.Seal.Index), candidate.Seal.RootHash)
	if err != nil {
		return err
	}
	if root.SQLExecutionPolicy != int(policy) || root.Index != uint64(candidate.Seal.Index) || root.Generation != uint64(candidate.Seal.Index) || root.Hash != candidate.Seal.StateHash {
		return fmt.Errorf("managed append-only candidate root does not match its QCKP")
	}
	if policy == sqlpolicy.ManagedGraphRequestMaterializationPolicy {
		if len(root.Files) != 2 || root.Files[0].Role != checkpoint.RoleSQLite || root.Files[1].Role != checkpoint.RoleGraphData {
			return fmt.Errorf("managed Graph candidate is not the exact immutable pair")
		}
	} else if policy == sqlpolicy.ManagedRequestMaterializationPolicy {
		if len(root.Files) != 1 || root.Files[0].Role != checkpoint.RoleSQLite {
			return fmt.Errorf("managed request candidate is not a SQLite-only root")
		}
	} else {
		return fmt.Errorf("unsupported managed append-only candidate policy")
	}
	if candidate.DecisionSlot <= candidate.Seal.Index || candidate.Seal.GenerationAnchorHash != ([32]byte{}) {
		return fmt.Errorf("managed append-only candidate slot or anchor is invalid")
	}
	if err := n.checkpoints.Verify(ctx, uint64(candidate.Seal.Index), candidate.Seal.RootHash, candidate.Seal.StateHash); err != nil {
		return fmt.Errorf("managed append-only immutable objects failed content verification: %w", err)
	}
	stage, err := os.MkdirTemp("", "rhiza-managed-append-only-candidate-*")
	if err != nil {
		return err
	}
	defer os.RemoveAll(stage)
	files, err := n.checkpoints.DownloadRootFiles(ctx, uint64(candidate.Seal.Index), candidate.Seal.RootHash, stage)
	if err != nil {
		return err
	}
	if err := n.validateCheckpointFiles(ctx, files, uint64(candidate.Seal.Index)); err != nil {
		return fmt.Errorf("managed append-only candidate contents are invalid: %w", err)
	}
	if n.core != nil {
		if err := n.verifyManagedAppendOnlyLocalHistory(candidate.Seal); err == nil {
			return ctx.Err()
		}
	}
	return n.verifyManagedAppendOnlyArchiveHistory(ctx, candidate.Seal)
}

func (n *Node) verifyManagedAppendOnlyLocalHistory(seal quepaxa.CheckpointSeal) error {
	core := n.core
	if core == nil {
		return fmt.Errorf("local Core history unavailable")
	}
	prefix, ok := core.PrefixHash(seal.Index)
	if !ok || prefix != seal.PrefixHash || core.ConfigIDForSlot(seal.Index+1) != seal.ConfigID {
		return fmt.Errorf("local certified cut prefix or config unavailable/mismatched")
	}
	next, following, err := core.CheckpointLeaderOrders(seal.Index)
	if err != nil || !reflect.DeepEqual(next, seal.NextLeaderOrder) || !reflect.DeepEqual(following, seal.FollowingLeaderOrder) {
		return fmt.Errorf("local certified cut leader order unavailable/mismatched: %w", err)
	}
	if n.config.EnableReconfiguration {
		history, err := core.CheckpointMembership(seal.Index)
		if err != nil || seal.Membership == nil || !reflect.DeepEqual(history, *seal.Membership) {
			return fmt.Errorf("local certified cut membership unavailable/mismatched: %w", err)
		}
	} else if seal.Membership != nil {
		return fmt.Errorf("fixed-membership candidate carries an unexpected history proof")
	}
	return nil
}

// When a Core has compacted away the candidate cut, verify the archive suffix
// with a real passive Core using the trusted bootstrap membership. The
// temporary recovery base is an already-certified archive base, not candidate
// validity or application installation authority.
func (n *Node) verifyManagedAppendOnlyArchiveHistory(ctx context.Context, seal quepaxa.CheckpointSeal) (resultErr error) {
	if n == nil || n.archive == nil || n.config == nil || seal.Index == 0 {
		return fmt.Errorf("managed append-only archive history verifier unavailable")
	}
	stage, err := os.MkdirTemp("", "rhiza-managed-append-only-history-*")
	if err != nil {
		return err
	}
	defer func() { resultErr = errors.Join(resultErr, os.RemoveAll(stage)) }()
	wal, err := qlog.Open(stage)
	if err != nil {
		return err
	}
	defer func() { resultErr = errors.Join(resultErr, wal.Close()) }()
	observerID := quepaxa.NodeID("rhiza-append-only-history-observer")
	for _, member := range n.config.Members {
		if member.ID == observerID {
			observerID = "rhiza-append-only-history-observer-1"
			break
		}
	}
	var validateValue func([]byte) error
	if n.materializationPolicy() == sqlpolicy.ManagedGraphRequestMaterializationPolicy {
		validateValue = types.ValidateManagedGraphExecutionPolicy
	} else {
		validateValue = types.ValidateManagedKVExecutionPolicy
	}
	observer, err := quepaxa.NewObserver(quepaxa.Config{NodeID: observerID, Cluster: *n.loadClusterConfig(), WAL: wal, ValidateValue: validateValue, EnableReconfiguration: n.config.EnableReconfiguration})
	if err != nil {
		return fmt.Errorf("create archive membership verifier: %w", err)
	}
	observer.SetCheckpointValidator(func(ctx context.Context, base quepaxa.CheckpointSeal) error {
		return n.checkpoints.Verify(ctx, uint64(base.Index), base.RootHash, base.StateHash)
	})
	var base quepaxa.Slot
	if baseSeal, baseDecision, ok := n.archive.RecoveryBase(); ok {
		if err := observer.RestoreCheckpointBase(ctx, baseSeal, baseDecision); err != nil {
			return fmt.Errorf("verify certified archive history base: %w", err)
		}
		base = baseSeal.Index
	}
	if seal.Index < base || seal.Index-base > 1<<20 {
		return fmt.Errorf("managed append-only candidate cut is outside bounded retained archive history")
	}
	for from := base + 1; from <= seal.Index; {
		limit := int(min(quepaxa.Slot(256), seal.Index-from+1))
		page, tip, pageErr := n.archive.DecisionsFrom(ctx, from, limit)
		if pageErr != nil || tip < seal.Index || len(page) != limit {
			return fmt.Errorf("managed append-only archive proof page unavailable: %w", pageErr)
		}
		for i, decision := range page {
			if decision.Slot != from+quepaxa.Slot(i) || sha256.Sum256(decision.Value) != decision.Hash || len(decision.Certificate) == 0 {
				return fmt.Errorf("managed append-only archive proof identity invalid at slot %d", decision.Slot)
			}
		}
		if err := observer.AcceptCertifiedValues(page); err != nil {
			return fmt.Errorf("verify managed append-only certified archive prefix: %w", err)
		}
		from += quepaxa.Slot(len(page))
	}
	prefix, ok := observer.PrefixHash(seal.Index)
	if !ok || prefix != seal.PrefixHash || observer.ConfigIDForSlot(seal.Index+1) != seal.ConfigID {
		return fmt.Errorf("managed append-only certified archive cut prefix/config differs")
	}
	next, following, err := observer.CheckpointLeaderOrders(seal.Index)
	if err != nil || !reflect.DeepEqual(next, seal.NextLeaderOrder) || !reflect.DeepEqual(following, seal.FollowingLeaderOrder) {
		return fmt.Errorf("managed append-only certified archive leader order differs: %w", err)
	}
	if n.config.EnableReconfiguration {
		history, historyErr := observer.CheckpointMembership(seal.Index)
		if historyErr != nil || seal.Membership == nil || !reflect.DeepEqual(history, *seal.Membership) {
			return errors.Join(fmt.Errorf("managed append-only certified archive membership differs"), historyErr)
		}
	} else if seal.Membership != nil {
		return fmt.Errorf("fixed-membership archive candidate carries an unexpected history proof")
	}
	return ctx.Err()
}

func (n *Node) selectManagedAppendOnlyRoot(ctx context.Context) (*checkpoint.Checkpoint, *quepaxa.SealedCheckpoint, error) {
	if n == nil || n.core == nil || n.checkpoints == nil || n.archive == nil || !isManagedAppendOnlyProtocol(n.managedStartup.Identity.ApplicationProtocol) {
		return nil, nil, fmt.Errorf("managed append-only checkpoint selection unavailable")
	}
	if err := n.archive.SyncThrough(ctx, n.core, n.core.Tip()); err != nil {
		return nil, nil, err
	}
	sealed, ok, err := n.core.LatestManagedCheckpointSeal()
	if err != nil {
		return nil, nil, err
	}
	locator, hasLocator, err := n.checkpoints.LoadManagedAppendOnlyLocator(ctx, n.materializationPolicy())
	if err != nil {
		return nil, nil, err
	}
	if hasLocator && (!ok || locator.Index > uint64(sealed.Index) || locator.Index == uint64(sealed.Index) && locator.SealDecisionSlot > uint64(sealed.DecisionSlot)) {
		// Another admitted voter may have advanced the shared append-only
		// archive after this startup's first archive snapshot. Refresh and
		// validate that certified prefix before treating CURRENT as a candidate
		// rank; the locator itself never supplies certificate authority.
		if err := n.refreshManagedAppendOnlyCertifiedPrefix(ctx); err != nil {
			return nil, nil, err
		}
		sealed, ok, err = n.core.LatestManagedCheckpointSeal()
		if err != nil {
			return nil, nil, err
		}
	}
	if !ok {
		if hasLocator {
			return nil, nil, fmt.Errorf("managed append-only CURRENT has no certified QCKP")
		}
		return nil, nil, nil
	}
	if hasLocator {
		if locator.Index > uint64(sealed.Index) || locator.Index == uint64(sealed.Index) && locator.SealDecisionSlot > uint64(sealed.DecisionSlot) {
			return nil, nil, fmt.Errorf("managed append-only CURRENT is ahead of actual local certified selection")
		}
		if locator.Index == uint64(sealed.Index) && locator.SealDecisionSlot == uint64(sealed.DecisionSlot) && (locator.RootHash != sealed.RootHash) {
			return nil, nil, fmt.Errorf("managed append-only CURRENT conflicts at the selected certificate rank")
		}
	}
	d, tip, err := n.archive.DecisionsFrom(ctx, sealed.DecisionSlot, 1)
	if err != nil || tip < sealed.DecisionSlot || len(d) != 1 || d[0].Slot != sealed.DecisionSlot {
		return nil, nil, fmt.Errorf("selected managed append-only QCKP is absent from durable archive: %w", err)
	}
	decision := d[0]
	if value, found := n.core.CertifiedValue(sealed.DecisionSlot); !found || value.Hash != decision.Hash || !bytes.Equal(value.Value, decision.Value) || sha256.Sum256(decision.Value) != decision.Hash || len(decision.Certificate) == 0 {
		return nil, nil, fmt.Errorf("selected managed append-only QCKP certificate differs from Core")
	}
	if err := n.core.ValidateManagedCheckpointRoot(ctx, quepaxa.ManagedCheckpointCandidate{
		Value: decision.Value, Hash: decision.Hash, Seal: sealed.CheckpointSeal,
		DecisionSlot: sealed.DecisionSlot, ConfigID: n.core.ConfigIDForSlot(sealed.DecisionSlot),
	}); err != nil {
		return nil, nil, err
	}
	root, err := n.checkpoints.OpenRoot(ctx, uint64(sealed.Index), sealed.RootHash)
	if err != nil {
		return nil, nil, err
	}
	if root.Hash != sealed.StateHash || root.SQLExecutionPolicy != int(n.materializationPolicy()) {
		return nil, nil, fmt.Errorf("selected managed append-only root differs from certified QCKP")
	}
	if hasLocator && locator.Index == uint64(sealed.Index) && locator.SealDecisionSlot == uint64(sealed.DecisionSlot) && locator.SealDecisionHash != decision.Hash {
		return nil, nil, fmt.Errorf("managed append-only CURRENT certificate hash differs")
	}
	return root, &sealed, nil
}

func (n *Node) refreshManagedAppendOnlyCertifiedPrefix(ctx context.Context) error {
	if n == nil || n.core == nil || n.archive == nil || n.core.CompactionFloor() != 0 {
		return fmt.Errorf("managed append-only startup prefix refresh is unavailable")
	}
	if err := n.archive.Load(ctx); err != nil {
		return fmt.Errorf("refresh managed append-only archive HEAD: %w", err)
	}
	if _, _, hasBase := n.archive.RecoveryBase(); hasBase {
		return fmt.Errorf("managed append-only archive unexpectedly has a recovery base")
	}
	tip := n.archive.Tip()
	for from := n.core.Tip() + 1; from <= tip; {
		page, observedTip, err := n.archive.DecisionsFrom(ctx, from, 256)
		if err != nil || observedTip < tip || len(page) == 0 {
			return fmt.Errorf("refreshed managed append-only archive prefix is incomplete: %w", err)
		}
		for i, decision := range page {
			if decision.Slot != from+quepaxa.Slot(i) || sha256.Sum256(decision.Value) != decision.Hash || len(decision.Certificate) == 0 {
				return fmt.Errorf("refreshed managed append-only archive identity is invalid at slot %d", decision.Slot)
			}
			if err := n.validateExecutionPolicy(decision.Value); err != nil {
				return fmt.Errorf("refreshed managed append-only decision policy is invalid at slot %d: %w", decision.Slot, err)
			}
		}
		if err := n.core.AcceptCertifiedValues(page); err != nil {
			return fmt.Errorf("accept refreshed managed append-only certified prefix: %w", err)
		}
		from += quepaxa.Slot(len(page))
	}
	return nil
}

func (n *Node) promoteManagedAppendOnlySeal(ctx context.Context, sealed quepaxa.SealedCheckpoint) error {
	if n == nil || n.core == nil || n.archive == nil || n.checkpoints == nil {
		return fmt.Errorf("managed append-only promotion unavailable")
	}
	if err := n.archive.SyncThrough(ctx, n.core, sealed.DecisionSlot); err != nil {
		return err
	}
	page, tip, err := n.archive.DecisionsFrom(ctx, sealed.DecisionSlot, 1)
	if err != nil || tip < sealed.DecisionSlot || len(page) != 1 {
		return fmt.Errorf("managed append-only certificate was not archived: %w", err)
	}
	d := page[0]
	if d.Slot != sealed.DecisionSlot || sha256.Sum256(d.Value) != d.Hash || d.Hash == (quepaxa.ValueHash{}) {
		return fmt.Errorf("managed append-only archived certificate identity differs")
	}
	decoded, checkpointValue, err := quepaxa.DecodeCheckpointSeal(d.Value)
	local, exists := n.core.CertifiedValue(sealed.DecisionSlot)
	if err != nil || !checkpointValue || decoded.Index != sealed.Index || decoded.RootHash != sealed.RootHash || !exists || local.Hash != d.Hash || !bytes.Equal(local.Value, d.Value) {
		return fmt.Errorf("managed append-only archive does not contain the selected exact QCKP")
	}
	if err := n.core.ValidateManagedCheckpointRoot(ctx, quepaxa.ManagedCheckpointCandidate{
		Value: d.Value, Hash: d.Hash, Seal: sealed.CheckpointSeal,
		DecisionSlot: sealed.DecisionSlot, ConfigID: n.core.ConfigIDForSlot(sealed.DecisionSlot),
	}); err != nil {
		return err
	}
	locator := checkpoint.ManagedAppendOnlyLocator{Policy: int(n.materializationPolicy()), Index: uint64(sealed.Index), RootHash: sealed.RootHash, SealDecisionSlot: uint64(d.Slot), SealDecisionHash: d.Hash}
	if err := n.checkpoints.PromoteManagedAppendOnlyLocator(ctx, locator); err != nil {
		if !errors.Is(err, checkpoint.ErrStaleCheckpoint) {
			return err
		}
		// A concurrent admitted publisher may have advanced CURRENT after this
		// exact seal was verified. Revalidate that newer locator against Core and
		// the archive; never roll CURRENT back or treat the hint as authority.
		_, newer, verifyErr := n.selectManagedAppendOnlyRoot(ctx)
		if verifyErr != nil || newer == nil || newer.Index < sealed.Index || newer.Index == sealed.Index && newer.DecisionSlot <= sealed.DecisionSlot {
			return errors.Join(err, verifyErr, fmt.Errorf("newer managed append-only CURRENT is not a verified higher-rank seal"))
		}
		if n.material != nil {
			if replayErr := n.replayLocalDecisions(ctx); replayErr != nil {
				return errors.Join(err, replayErr)
			}
		}
		if _, latest, verifyErr := n.selectManagedAppendOnlyRoot(ctx); verifyErr != nil || latest == nil || latest.Index < newer.Index || latest.Index == newer.Index && latest.DecisionSlot < newer.DecisionSlot {
			return errors.Join(err, verifyErr, fmt.Errorf("verified managed append-only root changed during stale promotion"))
		}
	}
	n.markManagedGraphInitialRootComplete()
	return nil
}

func (n *Node) startManagedAppendOnlyPublisher(ctx context.Context) {
	interval := n.config.CheckpointInterval
	if interval <= 0 {
		interval = 15 * time.Minute
	}
	period := min(interval, time.Minute)
	tail := n.config.CheckpointTailBytes
	if tail <= 0 {
		tail = 512 << 20
	}
	n.wg.Add(1)
	go func() {
		defer n.wg.Done()
		ticker := time.NewTicker(period)
		defer ticker.Stop()
		var last time.Time
		for {
			if last.IsZero() || time.Since(last) >= interval || n.wal.Bytes() >= tail {
				if err := n.publishManagedAppendOnlyCheckpoint(ctx); err != nil {
					if ctx.Err() == nil {
						log.Printf("managed append-only checkpoint publication deferred: %v", err)
					}
				} else {
					last = time.Now()
				}
			}
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}
	}()
}

func (n *Node) publishManagedAppendOnlyCheckpoint(ctx context.Context) error {
	if n == nil || n.managedStartup == nil || !isManagedAppendOnlyProtocol(n.managedStartup.Identity.ApplicationProtocol) || n.core == nil || n.material == nil || n.checkpoints == nil || n.archive == nil {
		return fmt.Errorf("managed append-only checkpoint runtime unavailable")
	}
	n.compactionMu.Lock()
	defer n.compactionMu.Unlock()
	if err := n.ensureInitialManagedAdmission(ctx); err != nil {
		return err
	}
	if !n.core.IsVoter() {
		return fmt.Errorf("managed append-only checkpoint requires an active voter")
	}
	if sealed, ok, err := n.core.LatestManagedCheckpointSeal(); err != nil {
		return err
	} else if ok {
		if err := n.promoteManagedAppendOnlySeal(ctx, sealed); err != nil {
			return err
		}
		if uint64(sealed.Index) >= n.material.StateTip() {
			return nil
		}
	}
	// A fresh empty Graph runtime has no positive materializer snapshot index.
	// Advance it with the existing certified read-barrier control before taking
	// the first immutable pair; this is not an application write or a genesis
	// substitute.
	if n.material.Tip() == 0 {
		if err := n.advanceManagedGraphCheckpointCut(ctx); err != nil {
			return err
		}
	}
	if err := n.replayLocalDecisions(ctx); err != nil {
		return err
	}
	files, cut, cleanup, err := n.material.CheckpointFilesAt(ctx)
	if err != nil {
		return err
	}
	defer cleanup()
	if cut == 0 || cut > uint64(n.core.Tip()) {
		return fmt.Errorf("managed append-only snapshot cut lies outside certified history")
	}
	actual := make([]checkpoint.File, 0, len(files))
	sources := make([]checkpoint.Source, 0, len(files))
	for _, file := range files {
		actual = append(actual, checkpoint.File{Role: string(file.Role), Path: file.Path})
		sources = append(sources, checkpoint.Source{Role: string(file.Role), Path: file.Path})
	}
	if err := n.validateCheckpointFiles(ctx, actual, cut); err != nil {
		return err
	}
	root, err := n.checkpoints.CreateManagedAppendOnlyCandidate(ctx, sources, cut)
	if err != nil {
		return err
	}
	if err := n.archive.SyncThrough(ctx, n.core, quepaxa.Slot(cut)); err != nil {
		return err
	}
	prefix, ok := n.core.PrefixHash(quepaxa.Slot(cut))
	if !ok {
		return fmt.Errorf("managed append-only checkpoint prefix unavailable")
	}
	next, following, err := n.core.CheckpointLeaderOrders(quepaxa.Slot(cut))
	if err != nil {
		return err
	}
	seal := quepaxa.CheckpointSeal{ConfigID: n.core.ConfigIDForSlot(quepaxa.Slot(cut)), Index: quepaxa.Slot(cut), RootHash: root.RootHash, StateHash: root.Hash, PrefixHash: prefix, NextLeaderOrder: next, FollowingLeaderOrder: following, GenerationAnchorHash: n.core.GenerationAnchorHash()}
	if n.config.EnableReconfiguration {
		history, historyErr := n.core.CheckpointMembership(seal.Index)
		if historyErr != nil {
			return historyErr
		}
		seal.Membership = &history
	}
	value, err := quepaxa.EncodeCheckpointSeal(seal)
	if err != nil {
		return err
	}
	if _, _, err := n.core.Propose(ctx, value); err != nil {
		return err
	}
	if err := n.replayLocalDecisions(ctx); err != nil {
		return err
	}
	sealed, exists, err := n.core.LatestManagedCheckpointSeal()
	if err != nil || !exists {
		return fmt.Errorf("managed append-only QCKP was not certified: %w", err)
	}
	return n.promoteManagedAppendOnlySeal(ctx, sealed)
}

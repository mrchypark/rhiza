package node

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"path"

	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/mrchypark/rhiza/pkg/recovery"
)

func (n *Node) verifyMembershipRepairProbe(ctx context.Context, request network.MembershipRepairProbeRequest) (network.MembershipRepairProbeResponse, error) {
	var response network.MembershipRepairProbeResponse
	if err := network.ValidateMembershipRepairProbeRequest(request); err != nil {
		return response, err
	}
	if n == nil || n.config == nil || n.core == nil || n.material == nil || n.server == nil || n.archive == nil || n.bucket == nil || n.transport == nil || n.fenceSession == nil || !n.ready.Load() {
		return response, fmt.Errorf("membership repair verifier is not a ready supervised voter")
	}
	identity := n.fenceSession.Identity()
	if identity.Validate() != nil || identity.WALIdentity == "" || identity.SourceClusterID != string(n.config.ClusterID) || identity.NodeID != string(n.config.NodeID) || identity.WALIdentity != n.core.WALIdentity() {
		return response, fmt.Errorf("membership repair verifier identity differs from its bound WAL")
	}
	beforeRevision := n.recoveryMembershipRevisionBefore()
	before := n.recoveryMembershipSnapshot()
	cluster := n.core.CurrentCluster()
	if !before.validFor(beforeRevision, string(n.config.NodeID)) || before.pending || before.configID != request.SourceConfigID || before.abortSlot != uint64(*request.AbortSlot) ||
		cluster.ConfigID != request.SourceConfigID || !clusterHasMember(cluster, quepaxa.NodeID(request.ExcludedOldMemberID)) || clusterHasMember(cluster, request.CandidateMember.ID) {
		return response, fmt.Errorf("membership repair source round or candidate differs from current voter state")
	}
	if err := ctx.Err(); err != nil {
		return response, err
	}
	readIndex, _, err := n.core.ReadIndexExcluding(ctx, quepaxa.NodeID(request.ExcludedOldMemberID))
	if err != nil {
		return response, fmt.Errorf("read source quorum excluding old member: %w", err)
	}
	if readIndex == 0 || !n.recoveryMembershipSnapshot().equal(before) || !sameRecoveryCluster(cluster, n.core.CurrentCluster()) {
		return response, fmt.Errorf("source membership changed during excluded read-index")
	}
	if err := n.catchUpRepairPrefix(ctx, readIndex, cluster); err != nil {
		return response, fmt.Errorf("catch up verifier through excluded read-index: %w", err)
	}
	nonce := network.MembershipRepairBarrierNonce(request.Challenge)
	barrierValue := types.EncodeReadBarrier(nonce)
	barrierSlot, decided := n.core.DecidedSlot(barrierValue)
	if !decided {
		barrierSlot, err = n.server.ProposeControl(ctx, barrierValue)
		if err != nil {
			return response, fmt.Errorf("commit membership repair read barrier: %w", err)
		}
	}
	if barrierSlot < readIndex || barrierSlot == 0 {
		return response, fmt.Errorf("membership repair barrier %d does not cover read-index %d", barrierSlot, readIndex)
	}
	decision, err := n.core.CompleteDecision(ctx, barrierSlot)
	if err != nil {
		return response, fmt.Errorf("complete membership repair barrier: %w", err)
	}
	certified, ok := n.core.CertifiedValue(barrierSlot)
	if !ok || certified.Slot != barrierSlot || decision.Slot != barrierSlot || !bytes.Equal(certified.Value, barrierValue) || !bytes.Equal(decision.Value, barrierValue) || len(certified.Certificate) == 0 {
		return response, fmt.Errorf("membership repair barrier is not the exact completed certified decision")
	}
	if err := n.core.EnsureDurableThrough(ctx, barrierSlot); err != nil {
		return response, fmt.Errorf("persist membership repair barrier: %w", err)
	}
	prefix, err := n.core.DurablePrefix(barrierSlot)
	if err != nil || prefix == ([32]byte{}) {
		if err == nil {
			err = fmt.Errorf("empty durable prefix")
		}
		return response, fmt.Errorf("persist membership repair prefix: %w", err)
	}
	if err := n.archive.SyncThrough(ctx, n.core, barrierSlot); err != nil {
		return response, fmt.Errorf("sync membership repair barrier to archive: %w", err)
	}
	if err := n.verifyMembershipRepairArchiveReadback(ctx, barrierSlot, certified); err != nil {
		return response, err
	}
	if err := n.transport.VerifyLearner(ctx, request.CandidateMember, barrierSlot, prefix); err != nil {
		return response, fmt.Errorf("verify candidate durable prefix: %w", err)
	}
	after := n.recoveryMembershipSnapshot()
	if !after.equal(before) || !sameRecoveryCluster(cluster, n.core.CurrentCluster()) || !n.ready.Load() || n.fenceSession.Identity() != identity ||
		after.pending || after.abortSlot != uint64(*request.AbortSlot) || after.configID != request.SourceConfigID {
		return response, fmt.Errorf("source voter or membership binding changed during candidate verification")
	}
	prefixHex := hex.EncodeToString(prefix[:])
	abortSlot := quepaxa.Slot(after.abortSlot)
	response = network.MembershipRepairProbeResponse{
		Version: request.Version, Challenge: request.Challenge, ScopeDigest: request.ScopeDigest,
		VerifierNodeID: identity.NodeID, VerifierWALIdentity: identity.WALIdentity,
		Ready: n.ready.Load(), Voting: after.voting, Pending: after.pending,
		SourceConfigID: cluster.ConfigID, SourceMembers: append([]quepaxa.Member(nil), cluster.Members...), AbortSlot: &abortSlot,
		ExcludedOldMemberID: request.ExcludedOldMemberID, ReadIndexSlot: uint64(readIndex), BarrierSlot: uint64(barrierSlot),
		BarrierNonce: hex.EncodeToString(nonce[:]), PrefixHash: prefixHex,
		CandidateMember: request.CandidateMember, CandidateBindRef: request.CandidateBindRef,
		CandidatePrefixThrough: uint64(barrierSlot), CandidatePrefixHash: prefixHex,
	}
	return response, nil
}

func (n *Node) catchUpRepairPrefix(ctx context.Context, through quepaxa.Slot, cluster quepaxa.Cluster) error {
	for _, member := range cluster.Members {
		if quepaxa.NodeID(member.ID) == n.core.NodeID() {
			continue
		}
		if err := n.catchUpPeer(ctx, n.transport, member.ID); err != nil {
			continue
		}
		if _, ok := n.core.PrefixHash(through); ok && quepaxa.Slot(n.material.Tip()) >= through {
			break
		}
	}
	if _, ok := n.core.PrefixHash(through); !ok || quepaxa.Slot(n.material.Tip()) < through {
		return fmt.Errorf("local certified/materialized prefix does not reach %d", through)
	}
	if err := n.core.EnsureDurableThrough(ctx, through); err != nil {
		return err
	}
	return nil
}

func (n *Node) verifyMembershipRepairArchiveReadback(ctx context.Context, slot quepaxa.Slot, expected quepaxa.DecidedValue) error {
	reader := recovery.NewManager(n.bucket, path.Join(n.config.ObjStorePrefix, string(n.config.ClusterID)), 1)
	defer reader.Close()
	if err := reader.Load(ctx); err != nil {
		return fmt.Errorf("reload membership repair archive: %w", err)
	}
	values, tip, err := reader.DecisionsFrom(ctx, slot, 1)
	if err != nil {
		return fmt.Errorf("read back membership repair barrier: %w", err)
	}
	if tip < slot || len(values) != 1 || values[0].Slot != slot || values[0].Hash != expected.Hash || !bytes.Equal(values[0].Value, expected.Value) || !bytes.Equal(values[0].Certificate, expected.Certificate) {
		return fmt.Errorf("membership repair archive barrier readback differs from exact completed decision")
	}
	return nil
}

func clusterHasMember(cluster quepaxa.Cluster, id quepaxa.NodeID) bool {
	for _, member := range cluster.Members {
		if member.ID == id {
			return true
		}
	}
	return false
}

func sameRecoveryCluster(a, b quepaxa.Cluster) bool {
	if a.ConfigID != b.ConfigID || len(a.Members) != len(b.Members) {
		return false
	}
	for i := range a.Members {
		if a.Members[i] != b.Members[i] {
			return false
		}
	}
	return true
}

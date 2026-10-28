package node

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"path"
	"strconv"

	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/fencing"
	"github.com/mrchypark/rhiza/pkg/recovery"
)

// initialBootstrapBarrier persists and reads back the one certified barrier
// associated with an exact initial assignment. The returned decimal slot is a
// lookup key only; the authority independently validates the archived record.
func (n *Node) initialBootstrapBarrier(ctx context.Context, assignment fencing.BootstrapAssignment, assignmentSetHash string) (string, error) {
	if err := assignment.Validate(); err != nil {
		return "", err
	}
	if assignment.Purpose != fencing.BootstrapInitial || n.config == nil || n.config.PreparedPodUID != assignment.PodUID ||
		string(n.config.ClusterID) != assignment.SourceClusterID || string(n.config.NodeID) != assignment.NodeID {
		return "", fmt.Errorf("bootstrap barrier assignment differs from this initial Node")
	}
	if err := validatePreparedGenesisRuntimeConfig(n.config); err != nil {
		return "", err
	}
	if n.fenceSession == nil || n.core == nil || n.server == nil || n.archive == nil || n.bucket == nil {
		return "", fmt.Errorf("bootstrap barrier requires a bound supervised Node and shared archive")
	}
	runtime := n.fenceSession.Identity()
	if runtime.SourceClusterID != assignment.SourceClusterID || runtime.NodeID != assignment.NodeID || runtime.PodName != assignment.PodName ||
		runtime.PodUID != assignment.PodUID || runtime.CredentialUID != assignment.CredentialUID || runtime.CredentialName != assignment.CredentialName ||
		runtime.WALIdentity == "" || runtime.WALIdentity != n.core.WALIdentity() || !n.core.IsVoter() {
		return "", fmt.Errorf("bootstrap barrier runtime identity is not the assigned bound voter")
	}
	digest, err := hex.DecodeString(assignmentSetHash)
	if err != nil || len(digest) != sha256.Size || hex.EncodeToString(digest) != assignmentSetHash {
		return "", fmt.Errorf("invalid bootstrap assignment-set hash")
	}
	nonce := fencing.BootstrapBarrierNonce(assignment.OperationID)
	return n.persistBootstrapBarrier(ctx, nonce)
}

func (n *Node) persistBootstrapBarrier(ctx context.Context, nonce [types.ReadBarrierNonceSize]byte) (string, error) {
	if n.core == nil || n.server == nil || n.archive == nil || n.bucket == nil || n.config == nil || !n.core.IsVoter() {
		return "", fmt.Errorf("bootstrap barrier requires a voter with shared archive")
	}
	value := types.EncodeReadBarrier(nonce)
	slot, decided := n.core.DecidedSlot(value)
	if !decided {
		var err error
		slot, err = n.server.ProposeControl(ctx, value)
		if err != nil {
			return "", fmt.Errorf("propose bootstrap read barrier: %w", err)
		}
	}
	if slot == 0 {
		return "", fmt.Errorf("bootstrap read barrier has no certified slot")
	}
	local, ok := n.core.CertifiedValue(slot)
	if !ok || local.Slot != slot || string(local.Value) != string(value) || local.Hash != sha256.Sum256(value) || len(local.Certificate) == 0 {
		return "", fmt.Errorf("bootstrap read barrier is not the exact local certified decision")
	}
	if err := n.core.EnsureDurableThrough(ctx, slot); err != nil {
		return "", fmt.Errorf("persist bootstrap barrier decision: %w", err)
	}
	if err := n.archive.SyncThrough(ctx, n.core, slot); err != nil {
		return "", fmt.Errorf("sync bootstrap barrier to archive: %w", err)
	}
	readback := recovery.NewManager(n.bucket, path.Join(n.config.ObjStorePrefix, string(n.config.ClusterID)), 1)
	defer readback.Close()
	if err := readback.Load(ctx); err != nil {
		return "", fmt.Errorf("reload bootstrap barrier archive: %w", err)
	}
	values, tip, err := readback.DecisionsFrom(ctx, slot, 1)
	if err != nil {
		return "", fmt.Errorf("read back bootstrap barrier archive: %w", err)
	}
	if tip < slot || len(values) != 1 || values[0].Slot != slot || values[0].Hash != local.Hash ||
		string(values[0].Value) != string(local.Value) || string(values[0].Certificate) != string(local.Certificate) {
		return "", fmt.Errorf("bootstrap barrier archive readback differs from the certified local decision")
	}
	return strconv.FormatUint(uint64(slot), 10), nil
}

func (n *Node) completeInitialBootstrap(ctx context.Context) error {
	if n.config == nil || n.config.PreparedPodUID == "" {
		return nil
	}
	if _, isDR, err := preparedDRAssignmentForRuntime(n.config); err != nil {
		return err
	} else if isDR {
		// DR activation is admitted against the live anchor before WAL binding;
		// it does not own the initial-genesis completion marker.
		return nil
	}
	if !preparedGenesisNodeCandidate(n.config) {
		return nil
	}
	if n.fenceSession == nil {
		return fmt.Errorf("prepared initial voter has no supervised runtime session")
	}
	marker, exists, err := readPreparedGenesisMarkerForRuntime(n.config.DataDir)
	if err != nil {
		return err
	}
	if !exists || marker.Assignment.Purpose != fencing.BootstrapInitial {
		return fmt.Errorf("initial bootstrap marker is missing or mismatched")
	}
	status, err := n.fenceSession.BootstrapStatus(ctx, marker.Assignment.OperationID)
	if err != nil {
		return fmt.Errorf("read initial bootstrap authority status: %w", err)
	}
	if status.Assignment == nil || *status.Assignment != marker.Assignment {
		return fmt.Errorf("initial bootstrap assignment is absent or changed")
	}
	if status.Complete {
		return nil
	}
	if status.AdmissionClosed {
		return fmt.Errorf("initial bootstrap admission is closed")
	}
	proof, err := n.initialBootstrapBarrier(ctx, marker.Assignment, status.AssignmentSetHash)
	if err != nil {
		return err
	}
	completion := fencing.BootstrapCompletion{
		OperationID: marker.Assignment.OperationID, AuthorizationHash: marker.Assignment.AuthorizationHash,
		AssignmentSetHash: status.AssignmentSetHash, CertifiedBarrierRef: proof,
	}
	if err := n.fenceSession.CompleteBootstrap(ctx, completion); err != nil {
		return fmt.Errorf("complete initial bootstrap: %w", err)
	}
	return nil
}

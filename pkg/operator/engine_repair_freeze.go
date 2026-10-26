package operator

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"

	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/fencing"
	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

// VerifyEngineRemovalFreeze rereads the pinned administrator Secret, then
// queries the original configuration directly. No request or proof supplied
// by the replacement candidate is accepted as evidence.
func (e *FencingExecutor) VerifyEngineRemovalFreeze(ctx context.Context, runtime fencing.RuntimeBinding, record fencing.EngineRepairRecord) error {
	if e == nil || ctx == nil || e.Kube == nil || e.AuthorityState == nil || record.Validate() != nil || record.RemovalOperation == nil ||
		runtime.StatefulSetUID != record.Binding.StatefulSetUID || runtime.SourceClusterID != record.Binding.SourceClusterID ||
		runtime.Validate(record.CurrentCandidateIdentity()) != nil || runtime.NodeID == record.Binding.OldMemberID {
		return fmt.Errorf("fencing: authenticated engine removal freeze authority is incomplete")
	}
	store, err := NewEngineRepairStore(e.AuthorityState, runtime.StatefulSetUID)
	if err != nil {
		return err
	}
	canonical, err := store.ReadEngineRepair(ctx, runtime.SourceClusterID, record.Binding.OperationID)
	if err != nil {
		return err
	}
	if canonical.Digest() != record.Digest() || canonical.IdentityDigest() != record.IdentityDigest() {
		return fmt.Errorf("fencing: engine removal freeze record is not the exact canonical reservation")
	}
	record = canonical
	token, err := e.readEngineRepairAdminToken(ctx, record)
	if err != nil {
		return fmt.Errorf("fencing: original quorum administrator credential is unavailable: %w", err)
	}
	transport := network.NewTransport(types.ClusterID(record.Binding.SourceClusterID), quepaxa.NodeID(runtime.NodeID), &record.SourceConfig, "")
	transport.SetAdminToken(token)
	defer func() {
		transport.SetAdminToken("")
		_ = transport.Close()
	}()
	freezeErr := func() error {
		proof, queryErr := transport.ReadEngineRemovalFreezeQuorum(ctx, *record.RemovalOperation)
		if queryErr != nil {
			return fmt.Errorf("fencing: direct original-quorum freeze query failed: %w", queryErr)
		}
		return e.verifyEngineRemovalFreezeEvidence(ctx, runtime, record, *record.RemovalOperation, record.SourceConfig, proof)
	}()
	if freezeErr == nil {
		return nil
	}
	// Once the REMOVE terminal commits, replicas intentionally discard their
	// active reconfiguration state. A retry may therefore no longer read the
	// active freeze. Accept only a separate quorum-signed, certified terminal
	// for the same original operation and exact canonical storage proof.
	if record.StorageProofDigest == "" {
		return freezeErr
	}
	terminalOperation := *record.RemovalOperation
	terminalOperation.FreezeHash = valueHashFromDigest(record.Binding.FreezeHash)
	terminalOperation.StorageEffectsExcludedHash = valueHashFromDigest(record.StorageProofDigest)
	if terminalOperation.FreezeHash == (quepaxa.ValueHash{}) || terminalOperation.StorageEffectsExcludedHash == (quepaxa.ValueHash{}) {
		return freezeErr
	}
	terminalProof, terminalErr := transport.ReadEngineRemovalTerminalQuorum(ctx, terminalOperation)
	if terminalErr == nil {
		terminalErr = network.VerifyEngineRemovalTerminalProof(terminalProof, record.Binding.SourceClusterID, record.SourceConfig, terminalOperation)
	}
	if terminalErr != nil {
		return fmt.Errorf("fencing: neither active freeze nor exact original-quorum REMOVE terminal was verified: %w", errors.Join(freezeErr, terminalErr))
	}
	return nil
}

func (e *FencingExecutor) readEngineRepairAdminToken(ctx context.Context, record fencing.EngineRepairRecord) (string, error) {
	credential := record.SourceAdminCredential
	if e == nil || e.Kube == nil || credential.Name == "" || credential.UID == "" || credential.AdminTokenDigest == "" {
		return "", fmt.Errorf("fencing: pinned original administrator credential reference is incomplete")
	}
	var secret object
	if err := e.Kube.Get(ctx, e.controller().corePath("secrets", credential.Name), &secret); err != nil {
		return "", err
	}
	if secret["type"] != "Opaque" || str(nested(secret, "metadata", "name")) != credential.Name ||
		str(nested(secret, "metadata", "namespace")) != e.Kube.Namespace ||
		str(nested(secret, "metadata", "uid")) != credential.UID {
		return "", fmt.Errorf("fencing: original administrator Secret identity differs from its canonical reference")
	}
	encoded := str(nested(secret, "data", "RHIZA_ADMIN_TOKEN"))
	token, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil || len(token) == 0 || fencing.EngineRepairAdminTokenDigest(token) != credential.AdminTokenDigest {
		return "", fmt.Errorf("fencing: original administrator Secret does not match its pinned digest")
	}
	return string(token), nil
}

// verifyEngineRemovalFreezeEvidence validates an authenticated quorum proof
// against the canonical source configuration, candidate runtime, and exact
// repair binding. The only caller is VerifyEngineRemovalFreeze, which obtains
// the proof from direct queries to the pinned original voters.
func (e *FencingExecutor) verifyEngineRemovalFreezeEvidence(
	ctx context.Context,
	runtime fencing.RuntimeBinding,
	record fencing.EngineRepairRecord,
	operation quepaxa.EngineRemoval,
	sourceConfig quepaxa.Cluster,
	proof network.EngineRemovalFreezeProof,
) error {
	if e == nil || ctx == nil || record.Validate() != nil || record.RemovalOperation == nil || uint64(sourceConfig.ConfigID) != record.Binding.OldConfigID ||
		sourceConfig.ConfigID != record.SourceConfig.ConfigID || !sameRepairSourceConfig(sourceConfig, record.SourceConfig) ||
		runtime.StatefulSetUID != record.Binding.StatefulSetUID || runtime.SourceClusterID != record.Binding.SourceClusterID ||
		runtime.Validate(record.CurrentCandidateIdentity()) != nil || operation.Version != 1 || operation.Mode != "engine-owned-remove" ||
		!fencing.SameEngineRemovalOperation(operation, *record.RemovalOperation) ||
		operation.OperationID != record.Binding.OperationID || uint64(operation.ExpectedConfigID) != record.Binding.OldConfigID ||
		uint64(operation.ExpectedAbortSlot) != record.Binding.ExpectedAbortSlot || operation.Removed.ID != quepaxa.NodeID(record.Binding.OldMemberID) ||
		operation.Removed.WALIdentity != record.Binding.OldWALIdentity || operation.Freeze != quepaxa.Slot(record.Binding.FreezeSlot) ||
		fmt.Sprintf("%x", operation.FreezeHash[:]) != record.Binding.FreezeHash || operation.StorageEffectsExcludedHash != (quepaxa.ValueHash{}) ||
		operation.Target.MemberSet()[quepaxa.NodeID(runtime.NodeID)].ID != "" || operation.Target.MemberSet()[operation.Removed.ID].ID != "" {
		return fmt.Errorf("fencing: authenticated engine removal freeze binding differs from repair authority")
	}
	if err := network.VerifyEngineRemovalFreezeProof(proof, record.Binding.SourceClusterID, sourceConfig, operation); err != nil {
		return fmt.Errorf("fencing: original-config durable freeze proof rejected: %w", err)
	}
	return nil
}

func sameRepairSourceConfig(left, right quepaxa.Cluster) bool {
	if left.ConfigID != right.ConfigID || len(left.Members) != len(right.Members) {
		return false
	}
	for id, member := range left.MemberSet() {
		if right.MemberSet()[id] != member {
			return false
		}
	}
	return true
}

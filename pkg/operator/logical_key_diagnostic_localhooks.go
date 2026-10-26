//go:build localhooks

package operator

import (
	"context"
	"errors"
	"fmt"
	"os"

	"github.com/mrchypark/rhiza/pkg/fencing"
)

// LogicalMutationKeySnapshot is a bounded, read-only view for localhooks
// diagnostics. It contains no object payloads, raw key-owner hashes, or writes.
type LogicalMutationKeySnapshot struct {
	Exists                    bool
	OwnerProducerUID          string
	OwnerSequence             uint64
	OwnerEntryFound           bool
	OwnerEntryTerminal        bool
	OwnerEntryRetired         bool
	CurrentTupleMatch         bool
	CurrentEntryTerminal      bool
	CurrentEntryRetired       bool
	PendingGenerationZero     bool
	GenerationZeroBarrier     bool
	OwnerReceiptStatus        string
	OwnerReceiptOutcome       fencing.MutationOutcome
	OwnerReceiptDigestMatches bool
	OwnerEntryStatus          string
}

const (
	logicalKeyReceiptAbsent    = "absent"
	logicalKeyReceiptReadError = "read-error"
	logicalKeyReceiptInvalid   = "invalid"
	logicalKeyReceiptValid     = "valid"
	logicalKeyEntryLive        = "live"
	logicalKeyEntryArchived    = "archived-not-inspected"
	logicalKeyEntryMissing     = "missing"
	logicalKeyEntryMismatch    = "mismatch"
)

// InspectLogicalMutationKeyLocalhooks reads the exact logical Run, its
// producer, the canonical key record, and the receipt named by its full owner.
// Archived entries are reported without traversal to preserve read-only
// behavior. It is available only in localhooks builds and performs no writes.
func (e *FencingExecutor) InspectLogicalMutationKeyLocalhooks(ctx context.Context, statefulSetUID string, identity fencing.Identity, sequence uint64, intent fencing.MutationIntent) (LogicalMutationKeySnapshot, error) {
	var snapshot LogicalMutationKeySnapshot
	if e == nil || e.AuthorityState == nil || ctx == nil || statefulSetUID == "" || identity.SourceClusterID == "" || identity.RunID == "" ||
		identity.WALIdentity == "" || sequence == 0 || intent.Key == "" || intent.StoreScopeHash == "" || intent.OperationID == "" {
		return snapshot, fmt.Errorf("fencing: logical key diagnostic identity is incomplete")
	}
	root, err := e.read(ctx, statefulSetUID, identity.SourceClusterID)
	if err != nil {
		return snapshot, err
	}
	if root.Ledger.Profile != fencing.LogicalWriterProfileV1 || len(root.Ledger.Runs) > fencing.MaxEngineRepairIncarnations {
		return snapshot, fmt.Errorf("fencing: logical key diagnostic profile or Run inventory is invalid")
	}
	currentRun, ok := root.Ledger.Runs[identity.RunID]
	if !ok || currentRun.Identity != identity || currentRun.LogicalBinding == nil || currentRun.Runtime != nil ||
		currentRun.LogicalBinding.ValidateBound() != nil || currentRun.LogicalBinding.Role != fencing.StartupRoleVoterActive ||
		currentRun.MutationProducerUID == "" || currentRun.MutationScopeHash == "" || intent.StoreScopeHash != currentRun.MutationScopeHash ||
		intent.OperationID != fencing.MutationRequestID(currentRun.MutationProducerUID, sequence) {
		return snapshot, fmt.Errorf("fencing: logical key diagnostic Run differs from canonical enrollment")
	}
	currentProducer, currentUID, _, err := e.AuthorityState.ReadLogicalProducer(ctx, statefulSetUID, *currentRun.LogicalBinding)
	if err != nil {
		return snapshot, err
	}
	if currentUID != currentRun.MutationProducerUID || currentProducer.WALIdentity != identity.WALIdentity || currentProducer.Validate() != nil {
		return snapshot, fmt.Errorf("fencing: logical key diagnostic producer differs from canonical Run")
	}
	currentEntry, currentEntryFound := currentProducer.Entries[sequence]
	if currentEntryFound && currentEntry.Intent != intent {
		return snapshot, fmt.Errorf("fencing: logical key diagnostic tuple differs from canonical producer")
	}
	currentOwner := (mutationExecutionAuthority{statefulSetUID: statefulSetUID, sourceClusterID: identity.SourceClusterID,
		runID: identity.RunID, logicalBinding: currentRun.LogicalBinding}).mutationOwner(currentRun, currentUID, sequence, intent)

	keyState, err := e.readMutationKeyState(ctx, intent.StoreScopeHash, intent.Key)
	if err != nil {
		if errors.Is(err, errAuthorityStateNotFound) {
			return snapshot, nil
		}
		return snapshot, err
	}
	snapshot.Exists = true
	snapshot.CurrentTupleMatch = keyState.Owner == currentOwner && keyState.Owner != ""
	snapshot.PendingGenerationZero = keyState.PendingGenerationZero != nil
	snapshot.GenerationZeroBarrier = keyState.Barrier != nil
	if currentEntryFound {
		snapshot.CurrentEntryTerminal = currentEntry.Resolution != nil && currentEntry.DispatchClosed
		snapshot.CurrentEntryRetired = sequence <= currentProducer.RetiredStageThrough || sequence <= currentProducer.CompactedThrough
	}
	if keyState.Owner == "" {
		return snapshot, nil
	}
	receipts, receiptStoreErr := e.receiptsForLogicalBinding(statefulSetUID, *currentRun.LogicalBinding)
	if receiptStoreErr != nil {
		snapshot.OwnerReceiptStatus = logicalKeyReceiptReadError
		return snapshot, nil
	}
	receipt, receiptErr := receipts.getContext(ctx, "receipt-"+keyState.Owner)
	if errors.Is(receiptErr, os.ErrNotExist) {
		snapshot.OwnerReceiptStatus = logicalKeyReceiptAbsent
		return snapshot, nil
	}
	if receiptErr != nil {
		if errors.Is(receiptErr, errInvalidMutationExecutionReceipt) {
			snapshot.OwnerReceiptStatus = logicalKeyReceiptInvalid
		} else {
			snapshot.OwnerReceiptStatus = logicalKeyReceiptReadError
		}
		return snapshot, nil
	}
	binding := receipt.LogicalBinding
	if receipt.AuthorityProfile != fencing.LogicalWriterProfileV1 || binding == nil || receipt.Runtime != (fencing.RuntimeBinding{}) ||
		binding.ValidateBound() != nil || binding.Role != fencing.StartupRoleVoterActive || binding.SourceClusterID != identity.SourceClusterID ||
		receipt.ProducerUID == "" || receipt.Sequence == 0 || receipt.WALIdentity != binding.WALIdentity ||
		receipt.ProducerUID != binding.ProducerUID || receipt.Intent.StoreScopeHash != intent.StoreScopeHash || receipt.Intent.Key != intent.Key ||
		receipt.Intent.OperationID != fencing.MutationRequestID(receipt.ProducerUID, receipt.Sequence) {
		snapshot.OwnerReceiptStatus = logicalKeyReceiptInvalid
		return snapshot, nil
	}
	ownerRun, exists := root.Ledger.Runs[binding.RunID]
	if !exists || ownerRun.LogicalBinding == nil || *ownerRun.LogicalBinding != *binding || ownerRun.Runtime != nil ||
		ownerRun.Identity.SourceClusterID != identity.SourceClusterID || ownerRun.Identity.WALIdentity != receipt.WALIdentity ||
		ownerRun.MutationProfile != fencing.MutationExecutionProfile || ownerRun.MutationProducerUID != receipt.ProducerUID ||
		ownerRun.MutationScopeHash != intent.StoreScopeHash {
		snapshot.OwnerReceiptStatus = logicalKeyReceiptInvalid
		return snapshot, nil
	}
	ownerAuthority := logicalMutationAuthority(statefulSetUID, *binding)
	computedOwner := ownerAuthority.mutationOwner(ownerRun, receipt.ProducerUID, receipt.Sequence, receipt.Intent)
	if computedOwner != keyState.Owner {
		snapshot.OwnerReceiptStatus = logicalKeyReceiptInvalid
		return snapshot, nil
	}
	ownerProducer, producerUID, _, producerErr := e.AuthorityState.ReadLogicalProducer(ctx, statefulSetUID, *binding)
	if producerErr != nil || producerUID != receipt.ProducerUID || ownerProducer.WALIdentity != receipt.WALIdentity || ownerProducer.Validate() != nil {
		snapshot.OwnerReceiptStatus = logicalKeyReceiptInvalid
		return snapshot, nil
	}
	snapshot.OwnerReceiptStatus = logicalKeyReceiptValid
	snapshot.OwnerReceiptOutcome = receipt.Outcome
	snapshot.OwnerProducerUID = receipt.ProducerUID
	snapshot.OwnerSequence = receipt.Sequence
	if receipt.Sequence <= ownerProducer.CompactedThrough {
		snapshot.OwnerEntryStatus = logicalKeyEntryArchived
		snapshot.OwnerEntryRetired = true
		return snapshot, nil
	}
	ownerEntry, ownerEntryFound := ownerProducer.Entries[receipt.Sequence]
	if !ownerEntryFound {
		snapshot.OwnerEntryStatus = logicalKeyEntryMissing
		snapshot.OwnerReceiptStatus = logicalKeyReceiptInvalid
		return snapshot, nil
	}
	if ownerEntry.Intent != receipt.Intent || !mutationReceiptMatchesDispatch(ownerEntry, receipt) {
		snapshot.OwnerEntryStatus = logicalKeyEntryMismatch
		snapshot.OwnerReceiptStatus = logicalKeyReceiptInvalid
		return snapshot, nil
	}
	snapshot.OwnerEntryStatus = logicalKeyEntryLive
	snapshot.OwnerReceiptDigestMatches = ownerEntry.Resolution != nil && ownerEntry.Resolution.ReceiptDigest == receipt.ReceiptDigest
	for _, dispatch := range ownerEntry.Dispatches {
		if dispatch.ReceiptDigest != "" && dispatch.ReceiptDigest != receipt.ReceiptDigest {
			snapshot.OwnerReceiptStatus = logicalKeyReceiptInvalid
			snapshot.OwnerReceiptDigestMatches = false
			return snapshot, nil
		}
		if dispatch.ReceiptDigest == receipt.ReceiptDigest {
			snapshot.OwnerReceiptDigestMatches = true
		}
	}
	snapshot.OwnerEntryFound = true
	snapshot.OwnerEntryTerminal = ownerEntry.Resolution != nil && ownerEntry.DispatchClosed
	snapshot.OwnerEntryRetired = receipt.Sequence <= ownerProducer.RetiredStageThrough
	return snapshot, nil
}

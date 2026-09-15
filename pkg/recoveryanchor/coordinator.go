package recoveryanchor

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"path"
	"strings"
)

type Coordinator struct {
	Backend        Backend
	EvidenceFormat string
	Verifier       func(context.Context, Request, Record) (Binding, []byte, error)
}

func validateHex64(s string) bool {
	b, err := hex.DecodeString(s)
	return err == nil && len(b) == 32
}

func (c *Coordinator) validateRequest(req Request) error {
	if c.Backend == nil {
		return fmt.Errorf("backend is required")
	}
	if c.Verifier == nil {
		return fmt.Errorf("verifier is required")
	}
	if c.EvidenceFormat == "" {
		return fmt.Errorf("evidence format is required")
	}
	if req.AnchorID == "" || req.OperationID == "" || req.StatefulSetUID == "" {
		return fmt.Errorf("anchor_id, operation_id, and stateful_set_uid are required")
	}
	if req.SourceClusterID == "" || req.TargetClusterID == "" {
		return fmt.Errorf("source_cluster_id and target_cluster_id are required")
	}
	if req.SourceClusterID == req.TargetClusterID {
		return fmt.Errorf("source and target clusters must differ")
	}
	if req.SourcePrefix == "" || req.TargetPrefix == "" {
		return fmt.Errorf("source_prefix and target_prefix are required")
	}
	// Reject dot, dotdot, traversal.
	if req.SourcePrefix == "." || req.SourcePrefix == ".." || (len(req.SourcePrefix) > 2 && req.SourcePrefix[:3] == "../") {
		return fmt.Errorf("source_prefix contains traversal")
	}
	if req.TargetPrefix == "." || req.TargetPrefix == ".." || (len(req.TargetPrefix) > 2 && req.TargetPrefix[:3] == "../") {
		return fmt.Errorf("target_prefix contains traversal")
	}

	// Canonical prefix: path.Clean(p) == p, no leading slash, no traversal.
	if req.SourcePrefix != path.Clean(req.SourcePrefix) || req.SourcePrefix[0] == '/' {
		return fmt.Errorf("source_prefix must be canonical (no leading slash, no traversal)")
	}
	if req.TargetPrefix != path.Clean(req.TargetPrefix) || req.TargetPrefix[0] == '/' {
		return fmt.Errorf("target_prefix must be canonical (no leading slash, no traversal)")
	}
	// Reject overlap: equal or ancestor.
	if req.SourcePrefix == req.TargetPrefix || strings.HasPrefix(req.SourcePrefix, req.TargetPrefix+"/") || strings.HasPrefix(req.TargetPrefix, req.SourcePrefix+"/") {
		return fmt.Errorf("source and target prefixes must not overlap")
	}
	if req.SourceMembership == "" {
		return fmt.Errorf("source_membership is required")
	}
	if req.Fork.Tip == 0 {
		return fmt.Errorf("fork tip is required")
	}
	if !validateHex64(req.Fork.ManifestHash) {
		return fmt.Errorf("fork manifest_hash must be 32-byte hex")
	}
	if !validateHex64(req.Fork.PrefixHash) {
		return fmt.Errorf("fork prefix_hash must be 32-byte hex")
	}
	if req.TargetMembership.Version != Version1 {
		return fmt.Errorf("target_membership version must be 1")
	}
	if req.TargetMembership.Cluster != req.TargetClusterID {
		return fmt.Errorf("target_membership.cluster must match target_cluster_id")
	}
	if !validateHex64(req.FenceHash) {
		return fmt.Errorf("fence_hash must be 32-byte hex")
	}
	if !validateHex64(req.TargetAnchorHash) {
		return fmt.Errorf("target_anchor_hash must be 32-byte hex")
	}
	return nil
}

const maxActivateAttempts = 8

func (c *Coordinator) Activate(ctx context.Context, req Request) (Receipt, error) {
	if err := c.validateRequest(req); err != nil {
		return Receipt{}, err
	}
	rhash, err := RequestHash(req)
	if err != nil {
		return Receipt{}, err
	}

	var lastErr error
	for attempt := 0; attempt < maxActivateAttempts; attempt++ {
		if ctx.Err() != nil {
			return Receipt{}, fmt.Errorf("context cancelled: %w", ctx.Err())
		}

		existing, v, err := c.Backend.Read(ctx, req.AnchorID)
		if err != nil {
			return Receipt{}, fmt.Errorf("read anchor: %w", err)
		}

		// Validate record schema always: Version1, EvidenceFormat matches,
		// nonempty binding (ClusterID + StorageID) and nonempty version string.
		if existing.Version != Version1 {
			return Receipt{}, fmt.Errorf("record version mismatch")
		}
		if existing.EvidenceFormat != c.EvidenceFormat {
			return Receipt{}, fmt.Errorf("evidence format mismatch")
		}
		if existing.Binding.ClusterID == "" || existing.Binding.StorageID == "" || v == "" {
			return Receipt{}, fmt.Errorf("record has empty binding or version")
		}

		// Check if already committed via helper (receipt == stored, no Transition).
		if committed(existing, req, rhash) {
			return *existing.LastTransition, nil
		}

		// Reject stale binding (source cluster changed).
		if existing.Binding.ClusterID != req.SourceClusterID {
			return Receipt{}, fmt.Errorf("stale source binding")
		}

		// Reject pending application write.
		if existing.PendingWrite {
			return Receipt{}, fmt.Errorf("pending application write")
		}

		// Reject generation overflow.
		if existing.Generation == ^uint64(0) {
			return Receipt{}, fmt.Errorf("generation overflow")
		}

		if existing.Transition == nil {
			// No pending transition: freeze current state and CAS reserve.
			frozenBinding := existing.Binding
			frozenGeneration := existing.Generation
			frozenEvidence := make([]byte, len(existing.Evidence))
			copy(frozenEvidence, existing.Evidence)
			pending := Record{
				Version:        Version1,
				EvidenceFormat: c.EvidenceFormat,
				Binding:        frozenBinding,
				Evidence:       frozenEvidence,
				Generation:     frozenGeneration,
				Transition: &Transition{
					Request:    req,
					Source:     frozenBinding,
					Generation: frozenGeneration,
					Evidence:   frozenEvidence,
				},
				LastTransition: deepCopyReceiptPtr(existing.LastTransition),
			}
			if err := c.Backend.CAS(ctx, req.AnchorID, v, pending); err != nil {
				lastErr = fmt.Errorf("CAS reserve: %w", err)
				continue
			}
			continue
		}

		// Transition exists: validate frozen state is intact.
		if err := validateFrozen(existing, req, rhash); err != nil {
			return Receipt{}, err
		}

		// Save frozen state before verifier callback (may mutate slices/pointers).
		frozenEvidenceCopy := bytes.Clone(existing.Transition.Evidence)
		baseGeneration := existing.Generation

		// Mandatory verifier: returns actual proof bytes and target binding.
		targetBinding, proofBytes, err := c.Verifier(ctx, req, existing)
		if err != nil {
			return Receipt{}, fmt.Errorf("verifier: %w", err)
		}
		if targetBinding.ClusterID != req.TargetClusterID || targetBinding.StorageID == "" {
			return Receipt{}, fmt.Errorf("verifier returned invalid target binding")
		}
		if !bytes.Equal(proofBytes, frozenEvidenceCopy) {
			return Receipt{}, fmt.Errorf("proof mismatch")
		}

		// Clone record, commit: replace binding, increment gen, clear transition.
		receipt := Receipt{
			AnchorID:    req.AnchorID,
			OperationID: req.OperationID,
			RequestHash: rhash,
			Target:      targetBinding,
			Generation:  baseGeneration + 1,
		}
		commitRec := Record{
			Version:        Version1,
			EvidenceFormat: c.EvidenceFormat,
			Binding:        targetBinding,
			Evidence:       frozenEvidenceCopy,
			Generation:     baseGeneration + 1,
			LastTransition: &receipt,
		}
		if err := c.Backend.CAS(ctx, req.AnchorID, v, commitRec); err != nil {
			lastErr = fmt.Errorf("CAS commit: %w", err)
			continue
		}
		continue
	}

	if lastErr != nil {
		return Receipt{}, lastErr
	}
	return Receipt{}, fmt.Errorf("activate: exceeded max attempts")
}

func committed(record Record, req Request, rhash string) bool {
	lt := record.LastTransition
	if lt == nil || record.Transition != nil || record.Version != Version1 {
		return false
	}
	return lt.RequestHash == rhash &&
		lt.AnchorID == req.AnchorID &&
		lt.OperationID == req.OperationID &&
		lt.Target.ClusterID == req.TargetClusterID &&
		lt.Generation == record.Generation &&
		record.Binding == lt.Target
}

func validateFrozen(record Record, req Request, rhash string) error {
	if record.Transition == nil {
		return fmt.Errorf("no pending transition")
	}
	frozenHash, err := RequestHash(record.Transition.Request)
	if err != nil {
		return fmt.Errorf("hash frozen request: %w", err)
	}
	if frozenHash != rhash {
		return fmt.Errorf("frozen request hash mismatch")
	}
	if record.Binding != record.Transition.Source {
		return fmt.Errorf("current binding does not match frozen source")
	}
	if record.Generation != record.Transition.Generation {
		return fmt.Errorf("current generation does not match frozen generation")
	}
	if !bytes.Equal(record.Evidence, record.Transition.Evidence) {
		return fmt.Errorf("current evidence does not match frozen evidence")
	}
	return nil
}

func (c *Coordinator) Verify(ctx context.Context, req Request, receipt Receipt) error {
	if c.Backend == nil {
		return fmt.Errorf("backend is required")
	}
	rhash, err := RequestHash(req)
	if err != nil {
		return err
	}
	record, _, err := c.Backend.Read(ctx, req.AnchorID)
	if err != nil {
		return fmt.Errorf("read anchor: %w", err)
	}
	if record.Version != Version1 || record.EvidenceFormat != c.EvidenceFormat {
		return fmt.Errorf("record schema mismatch")
	}
	if !committed(record, req, rhash) {
		return fmt.Errorf("record is not committed for this request")
	}
	if receipt != *record.LastTransition {
		return fmt.Errorf("receipt does not match stored state")
	}
	return nil
}

func CheckBinding(record Record, expected Binding, expectedGeneration uint64) error {
	if record.Version != Version1 {
		return fmt.Errorf("record version mismatch")
	}
	if record.PendingWrite {
		return fmt.Errorf("record has pending write")
	}
	if record.Transition != nil {
		return fmt.Errorf("record has pending transition")
	}
	if record.Binding != expected {
		return fmt.Errorf("binding mismatch")
	}
	if record.Generation != expectedGeneration {
		return fmt.Errorf("generation mismatch")
	}
	return nil
}

func UpdateApplication(ctx context.Context, b Backend, id string, expectedVersion string, expectedBinding Binding, expectedGeneration uint64, evidence []byte, pendingWrite bool) error {
	if expectedVersion == "" || expectedBinding.ClusterID == "" {
		return fmt.Errorf("expected version and binding are required")
	}
	record, v, err := b.Read(ctx, id)
	if err != nil {
		return fmt.Errorf("read anchor: %w", err)
	}
	if record.Version != Version1 {
		return fmt.Errorf("record version mismatch")
	}
	if v != expectedVersion {
		return fmt.Errorf("version mismatch: got %q want %q", v, expectedVersion)
	}
	if record.Binding != expectedBinding {
		return fmt.Errorf("binding mismatch")
	}
	if record.Generation != expectedGeneration {
		return fmt.Errorf("generation mismatch: got %d want %d", record.Generation, expectedGeneration)
	}
	if record.Transition != nil {
		return fmt.Errorf("record has active transition")
	}
	record.Evidence = evidence
	record.PendingWrite = pendingWrite
	if err := b.CAS(ctx, id, v, record); err != nil {
		return fmt.Errorf("CAS update: %w", err)
	}
	return nil
}

func deepCopyReceiptPtr(r *Receipt) *Receipt {
	if r == nil {
		return nil
	}
	cp := *r
	return &cp
}

package quepaxa

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
)

// EngineRemovalTerminalValue is the exact typed REMOVE terminal payload.
// It deliberately keeps the decided value separate from the operation tuple
// callers supplied when requesting a readback.
type EngineRemovalTerminalValue struct {
	Freeze       Slot          `json:"freeze"`
	TerminalSlot Slot          `json:"terminalSlot"`
	Target       Cluster       `json:"target"`
	Operation    EngineRemoval `json:"operation"`
}

// EngineRemovalTerminalSnapshot is a locally retained, durable terminal
// decision. It is not by itself proof of quorum-wide storage exclusion; a
// caller must verify it against the pinned original quorum.
type EngineRemovalTerminalSnapshot struct {
	ConfigID   uint          `json:"configID"`
	Operation  EngineRemoval `json:"operation"`
	Decision   DecidedValue  `json:"decision"`
	PrefixHash ValueHash     `json:"prefixHash"`
}

// DecodeEngineRemovalTerminal decodes only a committed engine-owned REMOVE
// terminal. Other consensus values return ok=false.
func DecodeEngineRemovalTerminal(value []byte) (EngineRemovalTerminalValue, bool, error) {
	control, ok, err := decodeReconfiguration(value)
	if err != nil || !ok {
		return EngineRemovalTerminalValue{}, ok, err
	}
	canonical, err := encodeReconfiguration(control)
	if err != nil || !bytes.Equal(canonical, value) {
		return EngineRemovalTerminalValue{}, true, fmt.Errorf("non-canonical engine removal terminal payload")
	}
	if !control.Terminal || control.Abort || control.EngineRemoval == nil {
		return EngineRemovalTerminalValue{}, false, nil
	}
	return EngineRemovalTerminalValue{
		Freeze: control.Freeze, TerminalSlot: control.TerminalSlot,
		Target: cloneCluster(control.Target), Operation: *cloneEngineRemoval(control.EngineRemoval),
	}, true, nil
}

// EngineRemovalTerminalSnapshot synchronizes and returns the exact durable
// terminal for the supplied operation. CertifiedValue provides the retained
// value/certificate; the method checks both before returning a typed payload.
func (c *Core) EngineRemovalTerminalSnapshot(ctx context.Context, expected EngineRemoval) (EngineRemovalTerminalSnapshot, error) {
	if c == nil || ctx == nil || expected.Version != 1 || expected.Mode != engineRemovalMode || expected.OperationID == "" ||
		expected.ExpectedConfigID == 0 || expected.Freeze == 0 || expected.FreezeHash == (ValueHash{}) ||
		expected.StorageEffectsExcludedHash == (ValueHash{}) || expected.Target.ConfigID == 0 {
		return EngineRemovalTerminalSnapshot{}, fmt.Errorf("engine removal terminal tuple is incomplete")
	}
	if expected.Freeze > ^Slot(0)-16 {
		return EngineRemovalTerminalSnapshot{}, fmt.Errorf("engine removal terminal slot overflows")
	}
	terminalSlot := expected.Freeze + 16
	if err := c.EnsureDurableThrough(ctx, terminalSlot); err != nil {
		return EngineRemovalTerminalSnapshot{}, err
	}
	config := c.clusterForSlot(terminalSlot)
	if config.ConfigID != expected.ExpectedConfigID {
		return EngineRemovalTerminalSnapshot{}, fmt.Errorf("engine removal terminal belongs to another configuration")
	}
	decision, ok := c.CertifiedValue(terminalSlot)
	if !ok || decision.Slot != terminalSlot || sha256.Sum256(decision.Value) != decision.Hash {
		return EngineRemovalTerminalSnapshot{}, fmt.Errorf("engine removal terminal is not the exact retained certified value")
	}
	certificateConfigID, certificate, err := decodeCertificate(decision.Certificate)
	if err != nil || certificateConfigID != config.ConfigID || certificate.Slot != terminalSlot || certificate.Proposal.Hash != decision.Hash {
		return EngineRemovalTerminalSnapshot{}, fmt.Errorf("engine removal terminal certificate does not match its retained value")
	}
	terminal, isTerminal, err := DecodeEngineRemovalTerminal(decision.Value)
	if err != nil || !isTerminal || terminal.Freeze != expected.Freeze || terminal.TerminalSlot != terminalSlot ||
		!sameCluster(terminal.Target, expected.Target) || !sameEngineRemoval(terminal.Operation, expected, true) {
		return EngineRemovalTerminalSnapshot{}, fmt.Errorf("engine removal terminal payload differs from the exact operation")
	}
	prefix, err := c.DurablePrefix(terminalSlot)
	if err != nil {
		return EngineRemovalTerminalSnapshot{}, err
	}
	return EngineRemovalTerminalSnapshot{
		ConfigID: config.ConfigID, Operation: terminal.Operation, Decision: decision, PrefixHash: ValueHash(prefix),
	}, nil
}

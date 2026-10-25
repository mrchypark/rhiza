package quepaxa

import (
	"context"
	"crypto/sha256"
	"fmt"
)

// EngineRemovalFreezeSnapshot is the locally retained, durable freeze value
// for an engine-owned REMOVE. It is not by itself proof of quorum-wide
// durability; callers must collect the same snapshot from an original-config
// quorum over authenticated peer connections.
type EngineRemovalFreezeSnapshot struct {
	ConfigID   uint          `json:"configID"`
	Operation  EngineRemoval `json:"operation"`
	Decision   DecidedValue  `json:"decision"`
	PrefixHash ValueHash     `json:"prefixHash"`
}

// EngineRemovalFreezeValue is the exact typed control carried by a freeze
// decision. It deliberately excludes terminal-only storage evidence.
type EngineRemovalFreezeValue struct {
	Freeze       Slot          `json:"freeze"`
	TerminalSlot Slot          `json:"terminalSlot"`
	Target       Cluster       `json:"target"`
	Operation    EngineRemoval `json:"operation"`
}

// DecodeEngineRemovalFreeze decodes only the typed, nonterminal engine-owned
// removal control. Other consensus values return ok=false.
func DecodeEngineRemovalFreeze(value []byte) (EngineRemovalFreezeValue, bool, error) {
	control, ok, err := decodeReconfiguration(value)
	if err != nil || !ok {
		return EngineRemovalFreezeValue{}, ok, err
	}
	if control.Terminal || control.Abort || control.EngineRemoval == nil {
		return EngineRemovalFreezeValue{}, false, nil
	}
	return EngineRemovalFreezeValue{Freeze: control.Freeze, TerminalSlot: control.TerminalSlot, Target: cloneCluster(control.Target), Operation: *cloneEngineRemoval(control.EngineRemoval)}, true, nil
}

// EngineRemovalFreezeSnapshot synchronizes and returns the exact active freeze
// only when the supplied tuple matches the in-flight operation. It never
// reconstructs an operation from summary state or completed membership history.
func (c *Core) EngineRemovalFreezeSnapshot(ctx context.Context, expected EngineRemoval) (EngineRemovalFreezeSnapshot, error) {
	if c == nil || ctx == nil || expected.Version != 1 || expected.Mode != engineRemovalMode || expected.OperationID == "" ||
		expected.ExpectedConfigID == 0 || expected.Freeze == 0 ||
		expected.FreezeHash == (ValueHash{}) || expected.StorageEffectsExcludedHash != (ValueHash{}) {
		return EngineRemovalFreezeSnapshot{}, fmt.Errorf("engine removal freeze tuple is incomplete")
	}
	if err := c.EnsureDurableThrough(ctx, expected.Freeze); err != nil {
		return EngineRemovalFreezeSnapshot{}, err
	}
	c.mu.RLock()
	state := c.reconfiguration
	if state == nil || state.engineRemoval == nil || state.freeze != expected.Freeze || state.id != expected.FreezeHash ||
		state.engineRemoval.Freeze != expected.Freeze || state.engineRemoval.OperationID != expected.OperationID ||
		state.engineRemoval.ExpectedConfigID != expected.ExpectedConfigID || state.engineRemoval.ExpectedAbortSlot != expected.ExpectedAbortSlot ||
		state.engineRemoval.Removed != expected.Removed || !sameCluster(state.engineRemoval.Target, expected.Target) ||
		state.engineRemoval.FreezeHash != (ValueHash{}) || state.engineRemoval.StorageEffectsExcludedHash != (ValueHash{}) {
		c.mu.RUnlock()
		return EngineRemovalFreezeSnapshot{}, fmt.Errorf("engine removal freeze is not the exact active operation")
	}
	decision, ok := c.decided[expected.Freeze]
	if !ok || !c.durable[expected.Freeze] || decision.Slot != expected.Freeze || decision.Hash != expected.FreezeHash || sha256.Sum256(decision.Value) != decision.Hash {
		c.mu.RUnlock()
		return EngineRemovalFreezeSnapshot{}, fmt.Errorf("engine removal freeze is not durably retained")
	}
	configID := c.clusterForSlotLocked(expected.Freeze).ConfigID
	control, isControl, err := decodeReconfiguration(decision.Value)
	if err != nil || !isControl || control.Terminal || control.Abort || control.Freeze != expected.Freeze ||
		control.TerminalSlot != expected.Freeze+16 || !sameCluster(control.Target, expected.Target) || control.EngineRemoval == nil ||
		control.EngineRemoval.OperationID != expected.OperationID || control.EngineRemoval.ExpectedConfigID != expected.ExpectedConfigID ||
		control.EngineRemoval.ExpectedAbortSlot != expected.ExpectedAbortSlot || control.EngineRemoval.Removed != expected.Removed ||
		!sameCluster(control.EngineRemoval.Target, expected.Target) || control.EngineRemoval.FreezeHash != (ValueHash{}) ||
		control.EngineRemoval.StorageEffectsExcludedHash != (ValueHash{}) {
		c.mu.RUnlock()
		return EngineRemovalFreezeSnapshot{}, fmt.Errorf("engine removal freeze value does not match its operation")
	}
	if configID != expected.ExpectedConfigID {
		c.mu.RUnlock()
		return EngineRemovalFreezeSnapshot{}, fmt.Errorf("engine removal freeze belongs to another configuration")
	}
	decision.Value = append([]byte(nil), decision.Value...)
	decision.Certificate = append([]byte(nil), decision.Certificate...)
	c.mu.RUnlock()
	prefix, err := c.DurablePrefix(expected.Freeze)
	if err != nil {
		return EngineRemovalFreezeSnapshot{}, err
	}
	return EngineRemovalFreezeSnapshot{ConfigID: expected.ExpectedConfigID, Operation: expected, Decision: decision, PrefixHash: ValueHash(prefix)}, nil
}

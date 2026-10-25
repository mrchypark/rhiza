package quepaxa

import (
	"context"
	"crypto/sha256"
	"testing"
)

func TestDecodeEngineRemovalTerminalRequiresExactTypedTerminal(t *testing.T) {
	cores, _, target := engineOwnedRemovalCluster(t)
	operation, err := cores["a"].BeginEngineOwnedRemovalAt(context.Background(), target, "remove-c-terminal-decode", 0)
	if err != nil {
		t.Fatal(err)
	}
	operation.StorageEffectsExcludedHash = ValueHash{7}
	value, err := encodeReconfiguration(reconfigurationValue{
		Terminal: true, Freeze: operation.Freeze, TerminalSlot: operation.Freeze + 16,
		Target: operation.Target, PrefixHash: ValueHash{1}, EngineRemoval: &operation,
	})
	if err != nil {
		t.Fatal(err)
	}
	decoded, ok, err := DecodeEngineRemovalTerminal(value)
	if err != nil || !ok || decoded.Freeze != operation.Freeze || decoded.TerminalSlot != operation.Freeze+16 ||
		!sameCluster(decoded.Target, operation.Target) || !sameEngineRemoval(decoded.Operation, operation, true) {
		t.Fatalf("decoded terminal=%+v ok=%v err=%v", decoded, ok, err)
	}
	if _, ok, err := DecodeEngineRemovalTerminal([]byte("ordinary consensus value")); err != nil || ok {
		t.Fatalf("ordinary value decoded as terminal: ok=%v err=%v", ok, err)
	}
	if _, _, err := DecodeEngineRemovalTerminal(append(append([]byte(nil), value...), ' ')); err == nil {
		t.Fatal("accepted trailing bytes after terminal payload")
	}
}

func TestEngineRemovalTerminalSnapshotReadsExactCertifiedValue(t *testing.T) {
	cores, _, target := engineOwnedRemovalCluster(t)
	ctx := context.Background()
	operation, err := cores["a"].BeginEngineOwnedRemovalAt(ctx, target, "remove-c-terminal-readback", 0)
	if err != nil {
		t.Fatal(err)
	}
	operation.StorageEffectsExcludedHash = ValueHash{9}
	for _, core := range cores {
		core.reconfigAdmission = func(context.Context, ReconfigurationAdmission) error { return nil }
	}
	if err := cores["b"].FinishEngineOwnedRemovalAt(ctx, operation); err != nil {
		t.Fatalf("commit terminal in fixture: %v", err)
	}
	snapshot, err := cores["a"].EngineRemovalTerminalSnapshot(ctx, operation)
	if err != nil {
		t.Fatalf("read exact terminal: %v", err)
	}
	if snapshot.ConfigID != operation.ExpectedConfigID || !sameEngineRemoval(snapshot.Operation, operation, true) ||
		snapshot.Decision.Slot != operation.Freeze+16 || snapshot.Decision.Hash == (ValueHash{}) || snapshot.PrefixHash == (ValueHash{}) {
		t.Fatalf("terminal snapshot=%+v", snapshot)
	}
	if snapshot.Decision.Hash != ValueHash(sha256.Sum256(snapshot.Decision.Value)) {
		t.Fatal("terminal snapshot hash does not bind the returned payload")
	}
	decoded, ok, err := DecodeEngineRemovalTerminal(snapshot.Decision.Value)
	if err != nil || !ok || !sameEngineRemoval(decoded.Operation, operation, true) {
		t.Fatalf("snapshot terminal payload differs: ok=%v err=%v", ok, err)
	}
	wrongOperation := operation
	wrongOperation.StorageEffectsExcludedHash = ValueHash{10}
	if _, err := cores["a"].EngineRemovalTerminalSnapshot(ctx, wrongOperation); err == nil {
		t.Fatal("accepted a terminal with another storage-exclusion hash")
	}
}

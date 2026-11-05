//go:build rhiza_local_testhooks

package node

import (
	"path/filepath"
	"testing"
)

// Exercise the same isolated reconstruction used by retained adoption, without
// the public FFI error sanitizer hiding the underlying native error.
func TestManagedGraphAdoptionReopenReconstructsCertifiedCheckpoint(t *testing.T) {
	f := newPreparedLearnerFixture(t)
	n := f.node
	base, root, ok := n.core.RecoveryRoot()
	if !ok || base == 0 || base != f.sealA.Index || root != f.sealA.RootHash {
		t.Fatal("fixture lacks the actual adopted nonzero checkpoint base")
	}
	before, _, err := n.inspectApplicationState(f.ctx, n.material)
	tip := n.material.Tip()
	if err != nil {
		t.Fatal("inspect actual applied Graph state", err)
	}
	prefix, ok := n.core.PrefixHash(n.core.Tip())
	if !ok || tip != uint64(n.core.Tip()) {
		t.Fatal("fixture lacks the exact certified/applied Graph prefix")
	}
	files, err := n.checkpoints.DownloadRootFiles(f.ctx, uint64(base), root, t.TempDir())
	if err != nil {
		t.Fatal("download actual adopted checkpoint pair", err)
	}
	actualRoot, err := n.checkpoints.OpenRoot(f.ctx, uint64(base), root)
	if err != nil {
		t.Fatal("read actual checkpoint metadata", err)
	}
	t.Logf("actual checkpoint root_config=%d seal_config=%d floor=%d applied=%d", actualRoot.ConfigID, f.sealA.ConfigID, base, tip)
	suffix, err := certifiedSuffixThrough(n.core, base+1, n.core.Tip())
	if err != nil {
		t.Fatal("read actual certified adoption suffix", err)
	}
	if err := n.compareManagedFileAtTip(f.ctx, before, tip, checkpointMaterializerFiles(files), suffix, filepath.Join(t.TempDir(), "expected.db")); err != nil {
		t.Fatalf("retained Graph adoption reconstruction inner error: %v", err)
	}
	after, _, err := n.inspectApplicationState(f.ctx, n.material)
	afterTip := n.material.Tip()
	afterPrefix, prefixOK := n.core.PrefixHash(n.core.Tip())
	if err != nil || after != before || afterTip != tip || !prefixOK || afterPrefix != prefix || n.core.CompactionFloor() != base {
		t.Fatalf("isolated reconstruction changed actual Graph state or certified history: tip=%d after=%d err=%v", tip, afterTip, err)
	}
	adoption, err := loadPreparedAdoption(filepath.Join(f.dataDir, "prepared-adoption.json"))
	if err != nil || adoption.BaseSeal == nil || adoption.BaseSeal.Index != base || adoption.BaseSeal.RootHash != root {
		t.Fatalf("actual persisted adoption lacks verified checkpoint base: %v", err)
	}
	// Match retained reopen's closed native handle; Shutdown cleanup may close
	// this already closed materializer again, as in existing fixture tests.
	if err := n.material.Close(); err != nil {
		t.Fatal("close actual Graph handle before retained-file verification", err)
	}
	if err := n.verifyExistingManagedSQLBeforeOpen(f.ctx, n.core, adoption); err != nil {
		t.Fatalf("full retained Graph adoption verification inner error: %v", err)
	}
	if n.core.CompactionFloor() != base || n.core.Tip() != adoption.Tip {
		t.Fatal("full verification changed actual adopted history")
	}
}

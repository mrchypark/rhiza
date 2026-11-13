package recovery

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/mrchypark/rhiza/internal/sqlpolicy"
	"github.com/mrchypark/rhiza/pkg/materializer"
)

func TestVerifyRestoredPreparedGraphBeforeAdoption(t *testing.T) {
	f := newGraphPreparedFixture(t)
	readonly := &haReadOnlyRecordingBucket{Bucket: f.bucket}
	view, err := OpenReadOnlyViewForApplicationProtocol(f.ctx, readonly, "mixed", 1, f.bootstrap, sqlpolicy.ManagedGraphRequestMaterializationPolicy, preparedGraphWindow, preparedGraphApplicationProtocol, true)
	if err != nil {
		t.Fatal(err)
	}
	identity := mixedPreparedIdentity()
	identity.Generation = 0
	handoff := filepath.Join(t.TempDir(), "prepared")
	if err := PublishPreparedStartupForPolicy(f.ctx, view, handoff, identity, sqlpolicy.ManagedGraphRequestMaterializationPolicy, preparedGraphWindow); err != nil {
		t.Fatal(err)
	}
	parsed, err := parsePreparedStartupDirectory(f.ctx, handoff, identity, sqlpolicy.ManagedGraphRequestMaterializationPolicy, preparedGraphWindow, preparedGraphApplicationProtocol)
	if err != nil {
		t.Fatal(err)
	}
	if parsed.handoffState == (materializer.ManagedApplicationState{}) || parsed.handoffTip != uint64(view.Tip()) {
		t.Fatalf("Graph handoff state was not inspected: state=%+v tip=%d want=%d", parsed.handoffState, parsed.handoffTip, view.Tip())
	}
	restored := filepath.Join(t.TempDir(), "restored")
	if _, err := LoadPreparedStartupForApplicationProtocol(f.ctx, readonly, "mixed", 1, f.bootstrap, handoff, restored, identity, sqlpolicy.ManagedGraphRequestMaterializationPolicy, preparedGraphWindow, preparedGraphApplicationProtocol, true); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"prepared-adoption.json", "prepared-adoption.pending"} {
		if _, err := os.Stat(filepath.Join(restored, name)); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("retry fixture unexpectedly contains %s", name)
		}
	}
	verified, err := VerifyRestoredPreparedStartupForPolicy(f.ctx, restored, handoff, view, identity, sqlpolicy.ManagedGraphRequestMaterializationPolicy, preparedGraphWindow, preparedGraphApplicationProtocol)
	if err != nil {
		t.Fatal(err)
	}
	if verified.Directory() != restored {
		t.Fatalf("verified restored directory=%q want=%q", verified.Directory(), restored)
	}
}

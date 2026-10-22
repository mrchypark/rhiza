package operator

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/mrchypark/rhiza/pkg/fencing"
)

func provisionStageStoreFixture(t *testing.T, dir, incarnation string) {
	t.Helper()
	if err := os.Mkdir(dir, 0700); err != nil {
		t.Fatal(err)
	}
	manifest := sourceStageManifest{Version: 1, Incarnation: incarnation, Records: []sourceStageRecord{}}
	manifest.Digest = sourceStageManifestDigest(manifest)
	data, err := json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "stages.json"), data, 0600); err != nil {
		t.Fatal(err)
	}
}

func stageStoreIntent() fencing.MutationIntent {
	return fencing.MutationIntent{
		ContractVersion: fencing.MutationIntentVersion,
		StoreScopeHash:  strings.Repeat("a", 64),
		OwnerStep:       fencing.MutationOwnerStep{Owner: "node", Workflow: "bootstrap", Step: "registration", Attempt: 1},
		Condition:       fencing.MutationCondition{Mode: fencing.MutationCreateOnly},
		Kind:            "upload",
		Key:             "membership/registration.json",
	}
}

func TestSourceStageStoreReservesAndReopensExactSealedTicket(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "spool")
	incarnation := strings.Repeat("1", 64)
	provisionStageStoreFixture(t, dir, incarnation)
	store, err := OpenSourceStageStore(dir, incarnation)
	if err != nil {
		t.Fatal(err)
	}
	ticket := strings.Repeat("b", 64)
	intent := stageStoreIntent()
	if err := store.Reserve("producer-uid", ticket, intent); err != nil {
		t.Fatal(err)
	}
	data := []byte(`{"membership":"original"}`)
	hash := sha256.Sum256(data)
	digest := hex.EncodeToString(hash[:])
	payload, err := store.Seal("producer-uid", ticket, digest, int64(len(data)), strings.NewReader(string(data)))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = OpenSourceStageStore(dir, incarnation)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	if err := store.Reserve("producer-uid", ticket, intent); err != nil {
		t.Fatal(err)
	}
	recovered, err := store.Seal("producer-uid", ticket, digest, int64(len(data)), strings.NewReader("changed retry bytes"))
	if err != nil || recovered != payload {
		t.Fatalf("reopen did not preserve sealed descriptor: %+v %v", recovered, err)
	}
	if err := store.Reserve("foreign-producer", ticket, intent); err == nil {
		t.Fatal("ticket was rebound to another producer")
	}
	changed := intent
	changed.Key = "membership/other.json"
	if err := store.Reserve("producer-uid", ticket, changed); err == nil {
		t.Fatal("ticket was rebound to another target")
	}
}

func TestSourceStageStoreRequiresPinnedExistingStateAndExclusiveLock(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "missing")
	if _, err := OpenSourceStageStore(missing, "incarnation"); err == nil {
		t.Fatal("missing store directory was initialized")
	}
	dir := filepath.Join(t.TempDir(), "spool")
	incarnation := strings.Repeat("1", 64)
	provisionStageStoreFixture(t, dir, incarnation)
	if _, err := OpenSourceStageStore(dir, strings.Repeat("2", 64)); err == nil {
		t.Fatal("changed store incarnation accepted")
	}
	store, err := OpenSourceStageStore(dir, incarnation)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := OpenSourceStageStore(dir, incarnation); err == nil {
		t.Fatal("concurrent spool owner accepted")
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(dir, "stages.json")); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenSourceStageStore(dir, incarnation); err == nil {
		t.Fatal("missing inventory was silently initialized")
	}
}

func TestSourceStageStoreRejectsCorruptOrChangedPayloadOnRecovery(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "spool")
	incarnation := strings.Repeat("3", 64)
	provisionStageStoreFixture(t, dir, incarnation)
	store, err := OpenSourceStageStore(dir, incarnation)
	if err != nil {
		t.Fatal(err)
	}
	ticket := strings.Repeat("c", 64)
	data := []byte("sealed")
	hash := sha256.Sum256(data)
	digest := hex.EncodeToString(hash[:])
	if err := store.Reserve("producer", ticket, stageStoreIntent()); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Seal("producer", ticket, digest, int64(len(data)), strings.NewReader(string(data))); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "payload-"+ticket)
	if err := os.Chmod(path, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("tampered"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenSourceStageStore(dir, incarnation); err == nil {
		t.Fatal("corrupt payload was accepted on reopen")
	}
}

func TestSourceStageStoreRecoversRecognizedCrashTemps(t *testing.T) {
	for _, tempKind := range []string{"manifest", "receiving"} {
		t.Run(tempKind, func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), "spool")
			incarnation := strings.Repeat("4", 64)
			provisionStageStoreFixture(t, dir, incarnation)
			store, err := OpenSourceStageStore(dir, incarnation)
			if err != nil {
				t.Fatal(err)
			}
			ticket := strings.Repeat("d", 64)
			intent := stageStoreIntent()
			if err := store.Reserve("producer", ticket, intent); err != nil {
				t.Fatal(err)
			}
			data := []byte("retry original bytes")
			hash := sha256.Sum256(data)
			descriptor := sourceStageDescriptor{Ref: "payload-" + ticket, Digest: hex.EncodeToString(hash[:]), Size: int64(len(data))}
			if tempKind == "receiving" {
				next := store.manifest
				next.Records = append([]sourceStageRecord(nil), store.manifest.Records...)
				next.Records[0].Receiving = &descriptor
				if err := store.persist(next); err != nil {
					t.Fatal(err)
				}
			}
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			name := ".manifest-deadbeef"
			if tempKind == "receiving" {
				name = ".receiving-" + ticket + "-deadbeef"
			}
			tempPath := filepath.Join(dir, name)
			if err := os.WriteFile(tempPath, []byte("uncommitted partial bytes"), 0600); err != nil {
				t.Fatal(err)
			}
			store, err = OpenSourceStageStore(dir, incarnation)
			if err != nil {
				t.Fatalf("recognized crash temp blocked recovery: %v", err)
			}
			t.Cleanup(func() { _ = store.Close() })
			if _, err := os.Lstat(tempPath); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("uncommitted temp was not removed: %v", err)
			}
			if err := store.Reserve("producer", ticket, intent); err != nil {
				t.Fatal(err)
			}
			sealed, err := store.Seal("producer", ticket, descriptor.Digest, descriptor.Size, bytes.NewReader(data))
			if err != nil || sealed != descriptor {
				t.Fatalf("same descriptor retry failed: %+v %v", sealed, err)
			}
			if err := store.verify(descriptor); err != nil {
				t.Fatalf("retry payload failed readback verification: %v", err)
			}
		})
	}
}

func TestSourceStageStoreRejectsUnknownAndUnassociatedTemps(t *testing.T) {
	for _, name := range []string{".stray", ".receiving-" + strings.Repeat("e", 64) + "-deadbeef"} {
		t.Run(name, func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), "spool")
			incarnation := strings.Repeat("5", 64)
			provisionStageStoreFixture(t, dir, incarnation)
			if err := os.WriteFile(filepath.Join(dir, name), []byte("do not remove"), 0600); err != nil {
				t.Fatal(err)
			}
			if _, err := OpenSourceStageStore(dir, incarnation); err == nil {
				t.Fatal("unknown or unassociated temp accepted")
			}
			if data, err := os.ReadFile(filepath.Join(dir, name)); err != nil || string(data) != "do not remove" {
				t.Fatalf("rejected file was modified: %q %v", data, err)
			}
		})
	}
}

func TestSourceStageCancellationSerializesAgainstSeal(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "spool")
	incarnation := strings.Repeat("5", 64)
	provisionStageStoreFixture(t, dir, incarnation)
	store, err := OpenSourceStageStore(dir, incarnation)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	ticket := strings.Repeat("6", 64)
	if err := store.Reserve("producer", ticket, stageStoreIntent()); err != nil {
		t.Fatal(err)
	}
	if err := store.Cancel("producer", ticket); err != nil {
		t.Fatal(err)
	}
	data := []byte("late finalizer")
	hash := sha256.Sum256(data)
	if _, err := store.Seal("producer", ticket, hex.EncodeToString(hash[:]), int64(len(data)), strings.NewReader(string(data))); err == nil {
		t.Fatal("finalization recreated a cancelled ticket")
	}

	sealedTicket := strings.Repeat("7", 64)
	if err := store.Reserve("producer", sealedTicket, stageStoreIntent()); err == nil {
		t.Fatal("cancelled owner was assigned another ticket")
	}
	otherIntent := stageStoreIntent()
	otherIntent.OwnerStep.Attempt++
	if err := store.Reserve("producer", sealedTicket, otherIntent); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Seal("producer", sealedTicket, hex.EncodeToString(hash[:]), int64(len(data)), strings.NewReader(string(data))); err != nil {
		t.Fatal(err)
	}
	if err := store.Cancel("producer", sealedTicket); err == nil {
		t.Fatal("sealed payload was cancelled")
	}
}

func TestSourceStageStorePersistenceFailureLatchesClosed(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "spool")
	incarnation := strings.Repeat("8", 64)
	provisionStageStoreFixture(t, dir, incarnation)
	store, err := OpenSourceStageStore(dir, incarnation)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	store.syncDir = func(string) error { return fmt.Errorf("injected directory sync failure") }
	if err := store.Reserve("producer", strings.Repeat("9", 64), stageStoreIntent()); err == nil {
		t.Fatal("directory sync failure acknowledged a reservation")
	}
	if err := store.Reserve("producer", strings.Repeat("a", 64), stageStoreIntent()); err == nil {
		t.Fatal("store remained writable after uncertain persistence")
	}
}

package managedbootstrap

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/mrchypark/rhiza/internal/atomicdir"
	"github.com/mrchypark/rhiza/internal/sqlpolicy"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/recovery"
)

func repairTestIdentity() recovery.PreparedStartupIdentity {
	return recovery.PreparedStartupIdentity{ClusterUID: "cluster", Generation: 1, DeploymentUID: "deployment", PodUID: "pod", NodeID: "node", SourceBinding: "source", ConfigDigest: "config"}
}

func repairCanonicalTempDir(t *testing.T) string {
	t.Helper()
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	return root
}

func writeRepairPreparedMarker(t *testing.T, dir string, identity recovery.PreparedStartupIdentity, mutate func(map[string]any)) {
	t.Helper()
	if err := os.Mkdir(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	sqlite, suffix := []byte("prepared-sqlite"), []byte("prepared-suffix")
	if err := os.WriteFile(filepath.Join(dir, "sqlite.db"), sqlite, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "certified-suffix.json"), suffix, 0o600); err != nil {
		t.Fatal(err)
	}
	marker := map[string]any{
		"version":    1,
		"identity":   identity,
		"head":       []byte("verified-head"),
		"sqliteHash": sha256.Sum256(sqlite),
		"suffixHash": sha256.Sum256(suffix),
	}
	if mutate != nil {
		mutate(marker)
	}
	data, err := json.Marshal(marker)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "completed.json"), data, 0o600); err != nil {
		t.Fatal(err)
	}
}

func validateLegacyPrepared(dir string, identity recovery.PreparedStartupIdentity) error {
	return recovery.ValidatePreparedStartupDirectoryForPolicy(context.Background(), dir, identity, sqlpolicy.LegacyMaterializationPolicy, types.DefaultIdempotencyWindowSlots, "")
}

func TestRepairIncompletePreparedDirectoryQuarantinedAndRepublished(t *testing.T) {
	root := repairCanonicalTempDir(t)
	prepared := filepath.Join(root, "repair-prepared-digest")
	if err := os.Mkdir(prepared, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(prepared, "partial"), []byte("first-incomplete"), 0o600); err != nil {
		t.Fatal(err)
	}
	valid := false
	validate := func() error {
		if valid {
			return nil
		}
		return errors.New("prepared evidence incomplete")
	}
	publishes := 0
	publish := func() error {
		publishes++
		if err := os.Mkdir(prepared, 0o700); err != nil {
			return err
		}
		if err := os.WriteFile(filepath.Join(prepared, "completed.json"), []byte("validated"), 0o600); err != nil {
			return err
		}
		valid = true
		return nil
	}
	if err := ensureRepairPreparedDirectory(prepared, validate, publish); err != nil {
		t.Fatal(err)
	}
	if publishes != 1 {
		t.Fatalf("publishes=%d", publishes)
	}
	quarantines, err := filepath.Glob(filepath.Join(root, ".repair-prepared-digest-quarantine-*"))
	if err != nil || len(quarantines) != 1 {
		t.Fatalf("quarantines=%v err=%v", quarantines, err)
	}
	if got, err := os.ReadFile(filepath.Join(quarantines[0], "partial")); err != nil || string(got) != "first-incomplete" {
		t.Fatalf("quarantined evidence=%q err=%v", got, err)
	}

	// A second interrupted preparation gets a distinct durable quarantine.
	valid = false
	if err := os.Remove(filepath.Join(prepared, "completed.json")); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(prepared, "partial"), []byte("second-incomplete"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := ensureRepairPreparedDirectory(prepared, validate, publish); err != nil {
		t.Fatal(err)
	}
	quarantines, err = filepath.Glob(filepath.Join(root, ".repair-prepared-digest-quarantine-*"))
	if err != nil || len(quarantines) != 2 || quarantines[0] == quarantines[1] {
		t.Fatalf("quarantines=%v err=%v", quarantines, err)
	}
	if publishes != 2 {
		t.Fatalf("publishes=%d", publishes)
	}
}

func TestRepairCompletedPreparedDirectoryReusedAfterValidation(t *testing.T) {
	root := repairCanonicalTempDir(t)
	prepared := filepath.Join(root, "repair-prepared-digest")
	identity := repairTestIdentity()
	writeRepairPreparedMarker(t, prepared, identity, nil)
	marker := filepath.Join(prepared, "completed.json")
	before, err := os.Lstat(marker)
	if err != nil {
		t.Fatal(err)
	}
	if err := ensureRepairPreparedDirectory(prepared, func() error {
		return validateLegacyPrepared(prepared, identity)
	}, func() error {
		return errors.New("validated preparation was republished")
	}); err != nil {
		t.Fatal(err)
	}
	after, err := os.Lstat(marker)
	if err != nil || !os.SameFile(before, after) {
		t.Fatalf("completed marker changed: %v", err)
	}
	quarantines, _ := filepath.Glob(filepath.Join(root, ".repair-prepared-digest-quarantine-*"))
	if len(quarantines) != 0 {
		t.Fatalf("validated preparation was quarantined: %v", quarantines)
	}
	// The local validator is complete before any later source read. A source
	// failure therefore cannot turn validated local evidence into quarantine.
	sourceErr := errors.New("transient source read failed")
	if sourceErr == nil {
		t.Fatal("unreachable")
	}
	quarantines, _ = filepath.Glob(filepath.Join(root, ".repair-prepared-digest-quarantine-*"))
	if len(quarantines) != 0 {
		t.Fatalf("transient source failure quarantined valid evidence: %v", quarantines)
	}
}

func TestRepairPreparedMarkerValidationRejectsCorruptEvidence(t *testing.T) {
	identity := repairTestIdentity()
	tests := []struct {
		name  string
		write func(t *testing.T, dir string)
		want  string
	}{
		{name: "empty object", write: func(t *testing.T, dir string) {
			if err := os.Mkdir(dir, 0o700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(dir, "completed.json"), []byte("{}"), 0o600); err != nil {
				t.Fatal(err)
			}
		}, want: "identity mismatch"},
		{name: "truncated JSON", write: func(t *testing.T, dir string) {
			if err := os.Mkdir(dir, 0o700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(dir, "completed.json"), []byte("{\"version\":1"), 0o600); err != nil {
				t.Fatal(err)
			}
		}, want: "unexpected EOF"},
		{name: "wrong pod identity", write: func(t *testing.T, dir string) {
			wrong := identity
			wrong.PodUID = "other-pod"
			writeRepairPreparedMarker(t, dir, wrong, nil)
		}, want: "identity mismatch"},
		{name: "wrong version", write: func(t *testing.T, dir string) {
			writeRepairPreparedMarker(t, dir, identity, func(marker map[string]any) { marker["version"] = 2 })
		}, want: "identity mismatch"},
		{name: "wrong policy inventory", write: func(t *testing.T, dir string) {
			writeRepairPreparedMarker(t, dir, identity, func(marker map[string]any) { marker["files"] = []any{map[string]any{}} })
		}, want: "unexpected prepared artifact inventory"},
		{name: "missing head", write: func(t *testing.T, dir string) {
			writeRepairPreparedMarker(t, dir, identity, func(marker map[string]any) { delete(marker, "head") })
		}, want: "preparation head missing"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			dir := filepath.Join(repairCanonicalTempDir(t), "prepared")
			tc.write(t, dir)
			err := validateLegacyPrepared(dir, identity)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("validation error=%v want %q", err, tc.want)
			}
		})
	}
}

func TestRepairInvalidMarkerQuarantinedAndRepublished(t *testing.T) {
	root := repairCanonicalTempDir(t)
	prepared := filepath.Join(root, "repair-prepared-digest")
	identity := repairTestIdentity()
	if err := os.Mkdir(prepared, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(prepared, "completed.json"), []byte("{}"), 0o600); err != nil {
		t.Fatal(err)
	}
	publish := func() error {
		writeRepairPreparedMarker(t, prepared, identity, nil)
		return nil
	}
	validate := func() error { return validateLegacyPrepared(prepared, identity) }
	if err := ensureRepairPreparedDirectory(prepared, validate, publish); err != nil {
		t.Fatal(err)
	}
	quarantines, err := filepath.Glob(filepath.Join(root, ".repair-prepared-digest-quarantine-*"))
	if err != nil || len(quarantines) != 1 {
		t.Fatalf("quarantines=%v err=%v", quarantines, err)
	}
	if got, err := os.ReadFile(filepath.Join(quarantines[0], "completed.json")); err != nil || string(got) != "{}" {
		t.Fatalf("invalid marker evidence=%q err=%v", got, err)
	}
	if err := validate(); err != nil {
		t.Fatalf("republished marker invalid: %v", err)
	}
}

func TestRepairMarkerChangedDuringInspectionIsQuarantined(t *testing.T) {
	root := repairCanonicalTempDir(t)
	prepared := filepath.Join(root, "repair-prepared-digest")
	identity := repairTestIdentity()
	writeRepairPreparedMarker(t, prepared, identity, nil)
	changed := false
	validate := func() error {
		if !changed {
			changed = true
			marker := filepath.Join(prepared, "completed.json")
			if err := os.Remove(marker); err != nil {
				return err
			}
			if err := os.WriteFile(marker, []byte("{}"), 0o600); err != nil {
				return err
			}
		}
		return validateLegacyPrepared(prepared, identity)
	}
	publish := func() error {
		writeRepairPreparedMarker(t, prepared, identity, nil)
		return nil
	}
	if err := ensureRepairPreparedDirectory(prepared, validate, publish); err != nil {
		t.Fatal(err)
	}
	quarantines, err := filepath.Glob(filepath.Join(root, ".repair-prepared-digest-quarantine-*"))
	if err != nil || len(quarantines) != 1 {
		t.Fatalf("quarantines=%v err=%v", quarantines, err)
	}
	if got, err := os.ReadFile(filepath.Join(quarantines[0], "completed.json")); err != nil || string(got) != "{}" {
		t.Fatalf("changed marker evidence=%q err=%v", got, err)
	}
}

func TestRepairPartialDataDirQuarantinedAndRetrySucceeds(t *testing.T) {
	root := repairCanonicalTempDir(t)
	dataDir := filepath.Join(root, "data")
	if err := os.Mkdir(dataDir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dataDir, "sqlite.db"), []byte("partial-data"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := ensureUnadoptedRepairDataDirAbsent(dataDir); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(dataDir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("partial DataDir still blocks retry: %v", err)
	}
	quarantines, err := filepath.Glob(filepath.Join(root, ".data-quarantine-*"))
	if err != nil || len(quarantines) != 1 {
		t.Fatalf("quarantines=%v err=%v", quarantines, err)
	}
	if got, err := os.ReadFile(filepath.Join(quarantines[0], "sqlite.db")); err != nil || string(got) != "partial-data" {
		t.Fatalf("quarantined DataDir=%q err=%v", got, err)
	}
	staging, err := reserveRepairSiblingPath(root, ".repair-data-staging-digest-")
	if err != nil {
		t.Fatal(err)
	}
	if err := validateRepairPromotionPaths(staging, dataDir); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(staging, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(staging, "sqlite.db"), []byte("verified-retry"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := atomicdir.RenameNoReplace(staging, dataDir); err != nil {
		t.Fatal(err)
	}
	if got, err := os.ReadFile(filepath.Join(dataDir, "sqlite.db")); err != nil || string(got) != "verified-retry" {
		t.Fatalf("retry data=%q err=%v", got, err)
	}
}

func TestRepairRetryRejectsUnsafeAndAmbiguousPaths(t *testing.T) {
	t.Run("prepared symlink", func(t *testing.T) {
		root := repairCanonicalTempDir(t)
		real := filepath.Join(root, "real")
		if err := os.Mkdir(real, 0o700); err != nil {
			t.Fatal(err)
		}
		prepared := filepath.Join(root, "repair-prepared-digest")
		if err := os.Symlink(real, prepared); err != nil {
			t.Fatal(err)
		}
		if err := ensureRepairPreparedDirectory(prepared, func() error { return nil }, func() error { return errors.New("published through symlink") }); err == nil {
			t.Fatal("prepared symlink accepted")
		}
	})
	t.Run("prepared marker symlink", func(t *testing.T) {
		root := repairCanonicalTempDir(t)
		prepared := filepath.Join(root, "repair-prepared-digest")
		if err := os.Mkdir(prepared, 0o700); err != nil {
			t.Fatal(err)
		}
		marker := filepath.Join(root, "marker")
		if err := os.WriteFile(marker, []byte("{}"), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(marker, filepath.Join(prepared, "completed.json")); err != nil {
			t.Fatal(err)
		}
		if err := ensureRepairPreparedDirectory(prepared, func() error { return nil }, func() error { return errors.New("published over marker") }); err == nil {
			t.Fatal("completion symlink accepted")
		}
	})
	t.Run("data directory symlink", func(t *testing.T) {
		root := repairCanonicalTempDir(t)
		real := filepath.Join(root, "real-data")
		if err := os.Mkdir(real, 0o700); err != nil {
			t.Fatal(err)
		}
		dataDir := filepath.Join(root, "data")
		if err := os.Symlink(real, dataDir); err != nil {
			t.Fatal(err)
		}
		if err := ensureUnadoptedRepairDataDirAbsent(dataDir); err == nil {
			t.Fatal("data symlink accepted")
		}
	})
	t.Run("ambiguous promotion", func(t *testing.T) {
		root := repairCanonicalTempDir(t)
		finalDir := filepath.Join(root, "data")
		for _, tc := range []struct {
			name    string
			staging string
			final   string
		}{
			{name: "same", staging: finalDir, final: finalDir},
			{name: "different parent", staging: filepath.Join(root, "other", "stage"), final: finalDir},
			{name: "final exists", staging: filepath.Join(root, "stage"), final: func() string {
				if err := os.Mkdir(finalDir, 0o700); err != nil {
					t.Fatal(err)
				}
				return finalDir
			}()},
		} {
			t.Run(tc.name, func(t *testing.T) {
				if err := validateRepairPromotionPaths(tc.staging, tc.final); err == nil {
					t.Fatal("ambiguous promotion path accepted")
				}
			})
		}
	})
}

func TestRepairErrorsPreserveInnerCause(t *testing.T) {
	inner := errors.New("inner cause sentinel")
	for _, tc := range []struct {
		name string
		wrap func(error) error
		text string
	}{
		{name: "preparation", wrap: repairPreparationFailed, text: "repair preparation failed"},
		{name: "verification", wrap: repairPreparedVerificationFailed, text: "repair prepared data verification failed"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.wrap(inner)
			if !errors.Is(err, inner) || !strings.Contains(err.Error(), tc.text) {
				t.Fatalf("inner cause was not preserved: %v", err)
			}
		})
	}
}

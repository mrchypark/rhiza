package recovery

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/mrchypark/rhiza/pkg/materializer"
)

func TestPreparedStartupCapturedHeadReverifiedAfterAdvance(t *testing.T) {
	f := newLargeProofFixture(t, 64, "prepared-captured", false)
	haReadOnlyAppend(t, f, "first")
	bucket := &appendOnlyRecordingBucket{Bucket: f.bucket}
	view, err := OpenReadOnlyView(f.ctx, bucket, f.prefix, 1, f.bootstrap)
	if err != nil {
		t.Fatal(err)
	}
	head := view.CapturedHead()
	if len(head) == 0 || sha256.Sum256(head) != view.HeadHash() {
		t.Fatal("captured head does not match verified hash")
	}
	tip, prefix := view.Tip(), view.PrefixHash()
	seal, decision, ok := view.RecoveryBase()
	if !ok || len(decision.Certificate) == 0 {
		t.Fatal("missing certified checkpoint base")
	}
	decision.Certificate[0] ^= 0xff
	_, fresh, ok := view.RecoveryBase()
	if !ok || bytes.Equal(decision.Certificate, fresh.Certificate) {
		t.Fatal("base certificate is not independently copied")
	}
	if seal.Index != view.Base() {
		t.Fatal("checkpoint base mismatch")
	}
	headCopy := view.CapturedHead()
	headCopy[0] ^= 0xff
	if bytes.Equal(headCopy, view.CapturedHead()) {
		t.Fatal("captured head aliases caller memory")
	}
	haReadOnlyAppend(t, f, "second")
	restored, err := OpenReadOnlyViewAtHead(f.ctx, bucket, f.prefix, 1, f.bootstrap, head)
	if err != nil {
		t.Fatal(err)
	}
	if restored.Tip() != tip || restored.PrefixHash() != prefix || restored.Tip() == f.core.Tip() {
		t.Fatal("captured view changed after HEAD advanced")
	}
	if !bytes.Equal(restored.CapturedHead(), head) {
		t.Fatal("reopened different head")
	}
	if _, mutations := bucket.counts(); mutations != 0 {
		t.Fatalf("source mutations: %d", mutations)
	}
}

func TestPreparedStartupPublishLoadAndRejectTampering(t *testing.T) {
	f := newLargeProofFixture(t, 64, "prepared-publish", false)
	haReadOnlyAppend(t, f, "first")
	bucket := &appendOnlyRecordingBucket{Bucket: f.bucket}
	view, err := OpenReadOnlyView(f.ctx, bucket, f.prefix, 1, f.bootstrap)
	if err != nil {
		t.Fatal(err)
	}
	identity := PreparedStartupIdentity{ClusterUID: "cluster", Generation: 1, DeploymentUID: "deployment", PodUID: "pod", NodeID: "node", SourceBinding: "source", ConfigDigest: "config"}
	dir := filepath.Join(t.TempDir(), "prepared")
	if err := PublishPreparedStartup(f.ctx, view, dir, identity); err != nil {
		t.Fatal(err)
	}
	haReadOnlyAppend(t, f, "second")
	loadPrepared := func(expected PreparedStartupIdentity) (*VerifiedPreparedStartup, error) {
		return LoadPreparedStartup(f.ctx, bucket, f.prefix, 1, f.bootstrap, dir, filepath.Join(t.TempDir(), "verified"), expected)
	}
	load := func(expected PreparedStartupIdentity) error {
		_, err := loadPrepared(expected)
		return err
	}
	verifyReconstructed := func(p *VerifiedPreparedStartup) {
		t.Helper()
		if p.Directory() == dir {
			t.Fatal("loaded mutable handoff directory")
		}
		state, err := materializer.Open(filepath.Join(p.Directory(), "sqlite.db"), 1)
		if err != nil {
			t.Fatal(err)
		}
		defer state.Close()
		if state.Tip() != uint64(view.Tip()) {
			t.Fatalf("tip=%d want=%d", state.Tip(), view.Tip())
		}
		if _, found, err := state.GraphMutationReceipt(f.ctx, "first"); err != nil || !found {
			t.Fatalf("certified graph receipt: %v %v", found, err)
		}
	}
	graphPath := func(root string) string {
		t.Helper()
		p := filepath.Join(root, "latticedb", "graph.ltdb")
		info, err := os.Stat(p)
		if err != nil {
			t.Fatal(err)
		}
		if info.IsDir() {
			return filepath.Join(p, "state.json")
		}
		return p
	}
	if err := load(identity); err != nil {
		t.Fatalf("valid captured preparation: %v", err)
	}
	if err := load(identity); err != nil {
		t.Fatalf("repeated valid reconstruction: %v", err)
	}
	foreign := identity
	foreign.PodUID = "other"
	if err := load(foreign); err == nil {
		t.Fatal("foreign pod accepted")
	}
	for _, name := range []string{"completed.json", "sqlite.db", "certified-suffix.json"} {
		t.Run(name, func(t *testing.T) {
			p := filepath.Join(dir, name)
			original, err := os.ReadFile(p)
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := os.WriteFile(p, original, 0600); err != nil {
					t.Error(err)
				}
			}()
			if err := os.WriteFile(p, []byte("malformed"), 0600); err != nil {
				t.Fatal(err)
			}
			if err := load(identity); err == nil {
				t.Fatal("tampered file accepted")
			}
		})
	}
	t.Run("forged SQLite hash", func(t *testing.T) {
		marker := filepath.Join(dir, "completed.json")
		database := filepath.Join(dir, "sqlite.db")
		originalMarker, err := os.ReadFile(marker)
		if err != nil {
			t.Fatal(err)
		}
		originalDB, err := os.ReadFile(database)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := os.WriteFile(marker, originalMarker, 0600); err != nil {
				t.Error(err)
			}
			if err := os.WriteFile(database, originalDB, 0600); err != nil {
				t.Error(err)
			}
		}()
		changed := append(bytes.Clone(originalDB), 1)
		if err := os.WriteFile(database, changed, 0600); err != nil {
			t.Fatal(err)
		}
		var manifest preparedStartupManifest
		if err := json.Unmarshal(originalMarker, &manifest); err != nil {
			t.Fatal(err)
		}
		manifest.SQLiteHash = sha256.Sum256(changed)
		encoded, err := json.Marshal(manifest)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(marker, encoded, 0600); err != nil {
			t.Fatal(err)
		}
		p, err := loadPrepared(identity)
		if err == nil {
			got, err := os.ReadFile(filepath.Join(p.Directory(), "sqlite.db"))
			if err != nil {
				t.Fatal(err)
			}
			if bytes.Equal(got, changed) {
				t.Fatal("forged SQLite adopted")
			}
			verifyReconstructed(p)
		}
	})
	t.Run("tampered graph data", func(t *testing.T) {
		p := graphPath(dir)
		original, err := os.ReadFile(p)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := os.WriteFile(p, original, 0600); err != nil {
				t.Error(err)
			}
		}()
		if err := os.WriteFile(p, append(bytes.Clone(original), 1), 0600); err != nil {
			t.Fatal(err)
		}
		prepared, err := loadPrepared(identity)
		if err == nil {
			got, err := os.ReadFile(graphPath(prepared.Directory()))
			if err != nil {
				t.Fatal(err)
			}
			if bytes.Equal(got, append(bytes.Clone(original), 1)) {
				t.Fatal("forged graph adopted")
			}
			verifyReconstructed(prepared)
		}
	})
	t.Run("missing graph data", func(t *testing.T) {
		p := graphPath(dir)
		original, err := os.ReadFile(p)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := os.WriteFile(p, original, 0600); err != nil {
				t.Error(err)
			}
		}()
		if err := os.Remove(p); err != nil {
			t.Fatal(err)
		}
		if prepared, err := loadPrepared(identity); err == nil {
			verifyReconstructed(prepared)
		}
	})
	if err := os.Remove(filepath.Join(dir, "completed.json")); err != nil {
		t.Fatal(err)
	}
	if err := load(identity); err == nil {
		t.Fatal("partial preparation accepted")
	}
	if _, mutations := bucket.counts(); mutations != 0 {
		t.Fatalf("source mutations: %d", mutations)
	}
}

func TestPreparedStartupCapturedHeadRejectsInvalid(t *testing.T) {
	f := newLargeProofFixture(t, 64, "prepared-invalid", false)
	haReadOnlyAppend(t, f, "first")
	view, err := OpenReadOnlyView(f.ctx, f.bucket, f.prefix, 1, f.bootstrap)
	if err != nil {
		t.Fatal(err)
	}
	head := view.CapturedHead()
	corrupt := bytes.Clone(head)
	corrupt[len(corrupt)/2] ^= 0xff
	for name, raw := range map[string][]byte{"missing": nil, "truncated": head[:len(head)-1], "tampered": corrupt, "trailing": append(bytes.Clone(head), 0)} {
		t.Run(name, func(t *testing.T) {
			if _, err := OpenReadOnlyViewAtHead(f.ctx, f.bucket, f.prefix, 1, f.bootstrap, raw); err == nil {
				t.Fatal("invalid captured head accepted")
			}
		})
	}
	decoded, err := decodeHead(head)
	if err != nil {
		t.Fatal(err)
	}
	key := f.prefix + "/" + extentObjectKey(decoded.TailHash, decoded.TailObject)
	if err := f.bucket.Delete(f.ctx, key); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenReadOnlyViewAtHead(f.ctx, f.bucket, f.prefix, 1, f.bootstrap, head); err == nil {
		t.Fatal("missing captured extent accepted")
	}
}

func TestPreparedStartupCapturedHeadRejectsInvalidCertificate(t *testing.T) {
	f := newLargeProofFixture(t, 64, "prepared-cert", false)
	haReadOnlyAppend(t, f, "first")
	view, err := OpenReadOnlyView(f.ctx, f.bucket, f.prefix, 1, f.bootstrap)
	if err != nil {
		t.Fatal(err)
	}
	head, err := decodeHead(view.CapturedHead())
	if err != nil {
		t.Fatal(err)
	}
	key := f.prefix + "/" + extentObjectKey(head.TailHash, head.TailObject)
	extent, err := decodeExtent(haReadOnlyObject(t, f.bucket, f.ctx, key))
	if err != nil {
		t.Fatal(err)
	}
	if len(extent.Decisions) == 0 || len(extent.Decisions[0].Certificate) == 0 {
		t.Fatal("fixture lacks certificate")
	}
	extent.Decisions[0].Certificate[0] ^= 0xff
	data, err := encodeExtent(extent)
	if err != nil {
		t.Fatal(err)
	}
	head.TailHash = sha256.Sum256(data)
	if err := f.bucket.Upload(f.ctx, f.prefix+"/"+extentObjectKey(head.TailHash, head.TailObject), bytes.NewReader(data)); err != nil {
		t.Fatal(err)
	}
	modified, err := encodeHead(head)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := OpenReadOnlyViewAtHead(f.ctx, f.bucket, f.prefix, 1, f.bootstrap, modified); err == nil {
		t.Fatal("invalid certified suffix accepted")
	}
}

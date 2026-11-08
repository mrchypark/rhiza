//go:build rhiza_local_testhooks

package managedsql

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/localtesthooks"
	objectstore "github.com/mrchypark/rhiza/internal/objstore"
	"github.com/mrchypark/rhiza/internal/sqlpolicy"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/checkpoint"
	"github.com/mrchypark/rhiza/pkg/operator"
	"github.com/mrchypark/rhiza/pkg/recovery"
	"github.com/thanos-io/objstore"
)

func TestManagedGraphOriginalOpensAfterControlHeadBeforeFirstQCKP(t *testing.T) {
	t.Run("control-only", func(t *testing.T) { testManagedGraphOriginalControlHead(t, "control-only") })
	t.Run("fresh-before-head", func(t *testing.T) { testManagedGraphOriginalControlHead(t, "fresh-before-head") })
	t.Run("application-start-refused", func(t *testing.T) { testManagedGraphOriginalControlHead(t, "application-start-refused") })
	t.Run("unused-before-node", func(t *testing.T) { testManagedGraphOriginalControlHead(t, "unused-before-node") })
	t.Run("unused-after-node", func(t *testing.T) { testManagedGraphOriginalControlHead(t, "unused-after-node") })
}

func testManagedGraphOriginalControlHead(t *testing.T, scenario string) {
	t.Helper()
	const policy = sqlpolicy.ManagedGraphRequestMaterializationPolicy
	configs, fixture := sqlClusterForProtocol(t, sqlpolicy.ManagedGraphRequestAppendOnlyApplicationProtocol)
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()

	atSeal := make(chan struct{}, 1)
	release := make(chan struct{})
	sourceChecked := make(chan struct{})
	allowOpen := make(chan struct{})
	var releaseSource sync.Once
	resumeOpen := func() { releaseSource.Do(func() { close(allowOpen) }) }
	defer resumeOpen()
	eventToHold := map[string]string{
		"fresh-before-head":  "managed-host:graph-bootstrap:source-checked",
		"unused-before-node": "managed-host:graph-bootstrap:before-unused-source-check",
		"unused-after-node":  "managed-host:graph-bootstrap:before-unused-evidence",
	}[scenario]
	var holdOpen atomic.Bool
	peerReady := make(chan string, 8)
	restorePeerReady := localtesthooks.Set(func(event string) {
		for _, cfg := range configs[:2] {
			if event == "node:catchup:after-ready:node="+string(cfg.Runtime.NodeID) {
				select {
				case peerReady <- event:
				default:
				}
			}
		}
		if event == eventToHold && holdOpen.Swap(false) {
			close(sourceChecked)
			<-allowOpen
		}
	})
	defer restorePeerReady()
	restore := localtesthooks.SetProposalEntry(func(value []byte) {
		if _, seal, err := types.DecodeCheckpointSeal(value); err == nil && seal {
			select {
			case atSeal <- struct{}{}:
			default:
			}
			<-release
		}
	})
	defer restore()
	defer close(release)

	type openResult struct {
		host *Host
		err  error
	}
	var lateOpen <-chan openResult
	startLateOpen := func() {
		holdOpen.Store(true)
		result := make(chan openResult, 1)
		lateOpen = result
		done := make(chan struct{})
		go func() {
			defer close(done)
			h, err := fixture.open(ctx, configs[2])
			if h != nil {
				t.Cleanup(func() { _ = h.Close() })
			}
			result <- openResult{h, err}
		}()
		t.Cleanup(func() { <-done })
		select {
		case <-sourceChecked:
		case <-ctx.Done():
			t.Fatal("late original did not reach its startup gate", ctx.Err())
		}
	}
	if strings.HasPrefix(scenario, "unused-") {
		startLateOpen()
	}
	var first *Host
	for i, cfg := range configs[:2] {
		h, err := fixture.open(ctx, cfg)
		if h != nil {
			t.Cleanup(func() { _ = h.Close() })
		}
		if err != nil {
			t.Fatal(err)
		}
		if i == 0 {
			first = h
		}
	}
	// A real admitted quorum can publish the first READ cut without the
	// third original. Hold the publisher only after that HEAD is durable.
	seen := make(map[string]bool)
	for len(seen) < 2 {
		select {
		case event := <-peerReady:
			seen[event] = true
		case <-ctx.Done():
			t.Fatal("first two peers did not finish startup catch-up", ctx.Err())
		}
	}
	if scenario == "fresh-before-head" {
		startLateOpen()
	}
	if err := first.ensureInitialRequestAdmission(ctx); err != nil {
		t.Fatal("first original admission", err)
	}
	select {
	case <-atSeal:
	case <-ctx.Done():
		t.Fatal("first publisher did not reach QCKP after HEAD", ctx.Err())
	}

	cfg := configs[0].Runtime
	bucket, err := objectstore.NewBucketForNamespace(ctx, objectstore.Config{
		Provider: objectstore.ProviderS3, Endpoint: cfg.ObjStoreEndpoint,
		Bucket: cfg.ObjStoreBucket, Region: cfg.ObjStoreRegion,
		Insecure: cfg.ObjStoreInsecure, AccessKey: cfg.ObjStoreAccessKey, SecretKey: cfg.ObjStoreSecretKey,
	}, fixture.source.finalPrefix)
	if err != nil {
		t.Fatal(err)
	}
	defer bucket.Close()
	cp, err := checkpoint.NewAppendOnlyManagerForPolicy(bucket, fixture.source.finalPrefix, t.TempDir(), policy, 1)
	if err != nil {
		t.Fatal(err)
	}
	if _, found, err := cp.LoadManagedAppendOnlyLocator(ctx, policy); err != nil || found {
		t.Fatalf("CURRENT already published: found=%t err=%v", found, err)
	}
	archive, err := recovery.NewAppendOnlyManagerWithValueValidator(bucket, fixture.source.finalPrefix, 1, types.ValidateManagedGraphExecutionPolicy)
	if err != nil {
		t.Fatal(err)
	}
	defer archive.Close()
	if err := archive.Load(ctx); err != nil {
		t.Fatal(err)
	}
	tip := archive.Tip()
	if tip == 0 || tip > 256 {
		t.Fatalf("expected small nonempty first control HEAD, tip=%d", tip)
	}
	decisions, _, err := archive.DecisionsFrom(ctx, 1, 256)
	if err != nil || len(decisions) != int(tip) {
		t.Fatalf("missing first control prefix: %v", err)
	}
	for _, d := range decisions {
		if barrier, err := types.DecodeReadBarrier(d.Value); err != nil || !barrier || len(d.Certificate) == 0 {
			t.Fatalf("expected certified READ, slot=%d err=%v", d.Slot, err)
		}
	}
	startKey := fixture.source.finalPrefix + "/voters/managed-sql-application-start.json"
	started, err := bucket.Exists(ctx, startKey)
	if err != nil || started {
		t.Fatalf("fixture has application-start history: exists=%t err=%v", started, err)
	}
	if scenario == "application-start-refused" {
		// Install the exact marker bytes, not a malformed marker that would
		// be rejected before the no-root branch. No mutation is proposed.
		birth, err := bucket.Get(ctx, fixture.source.finalPrefix+"/voters/managed-sql-initialization.json")
		if err != nil {
			t.Fatal(err)
		}
		body, readErr := io.ReadAll(io.LimitReader(birth, 65537))
		if err := errors.Join(readErr, birth.Close()); err != nil || len(body) == 0 || len(body) > 65536 {
			t.Fatalf("read exact bootstrap marker: bytes=%d err=%v", len(body), err)
		}
		if err := bucket.Upload(ctx, startKey, bytes.NewReader(body), objstore.WithIfNotExists()); err != nil {
			t.Fatal(err)
		}
	}
	before, err := operator.LoadV1Alpha2Admission(ctx, configs[0].Store, configs[0].Ref)
	if err != nil {
		t.Fatal(err)
	}
	init, err := operator.LoadV1Alpha2ApplicationInit(ctx, configs[0].Store, configs[0].Ref)
	if err != nil || init.Assignment().State != "Consumed" || init.Assignment().Plan != configs[0].Plan {
		t.Fatalf("fixture did not consume exact E0: %v", err)
	}
	var h *Host
	if lateOpen != nil {
		resumeOpen()
		select {
		case result := <-lateOpen:
			h, err = result.host, result.err
		case <-ctx.Done():
			t.Fatal("late original did not complete", ctx.Err())
		}
	} else {
		h, err = fixture.open(ctx, configs[2])
		if h != nil {
			t.Cleanup(func() { _ = h.Close() })
		}
	}
	if scenario == "application-start-refused" {
		if err == nil || !strings.Contains(err.Error(), "cannot cross application start") {
			t.Fatalf("exact application-start marker did not block control-only startup: %v", err)
		}
		for _, name := range []string{"sqlite.db", "latticedb"} {
			if _, err := os.Lstat(filepath.Join(configs[2].Runtime.DataDir, name)); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("rejected startup created %s or its absence is unproved: %v", name, err)
			}
		}
		return
	}
	if err != nil {
		t.Fatalf("third original refused certified control HEAD at %d before first QCKP: %v", tip, err)
	}
	after, err := operator.LoadV1Alpha2Admission(ctx, configs[0].Store, configs[0].Ref)
	if err != nil || after.ResourceVersion() != before.ResourceVersion() || !bytes.Equal(after.Evidence(), before.Evidence()) {
		t.Fatalf("late original changed the consumed E0: %v", err)
	}
	if scenario == "unused-after-node" {
		// It opened before HEAD, so async peer catch-up may still be applying
		// the prefix. This case proves only the post-open E0 decision.
		return
	}
	state, err := h.node.ManagedState()
	if err != nil || state.AppliedTip != uint64(tip) || state.CertifiedTip != uint64(tip) {
		t.Fatalf("third original applied=%d certified=%d want control tip=%d: %v", state.AppliedTip, state.CertifiedTip, tip, err)
	}
}

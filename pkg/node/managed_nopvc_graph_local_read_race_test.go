//go:build rhiza_local_testhooks

package node

import (
	"context"
	"encoding/hex"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/localtesthooks"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/qlog"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

func TestManagedNoPVCLocalGraphReadRechecksRootCompletedAfterInitialNotReady(t *testing.T) {
	cases := []struct {
		name       string
		complete   func(*testing.T, *managedGraphAppendOnlyAdmissionFixture, context.Context) <-chan error
		wantCancel bool
	}{
		{
			name: "actual-publisher-completes-root",
			complete: func(t *testing.T, fixture *managedGraphAppendOnlyAdmissionFixture, ctx context.Context) <-chan error {
				t.Helper()
				t.Cleanup(fixture.transport.releaseAll)
				published := make(chan error, 1)
				go func() { published <- fixture.node.publishManagedAppendOnlyCheckpoint(ctx) }()
				fixture.transport.mu.Lock()
				gate := fixture.transport.gates[9]
				fixture.transport.mu.Unlock()
				if gate == nil {
					t.Fatal("actual initial QCKP publisher has no slot-9 recorder gate")
				}
				select {
				case <-gate.entered:
				case <-ctx.Done():
					progress := fixture.node.core.RuntimeProgressSnapshot()
					select {
					case err := <-published:
						t.Fatalf("actual initial QCKP publisher returned before recorder gate: err=%v progress=%+v", err, progress)
					default:
						t.Fatalf("actual initial QCKP publisher did not reach slot-9 recorder gate before context ended: progress=%+v", progress)
					}
				}
				fixture.transport.releaseAll()
				select {
				case err := <-published:
					if err != nil {
						t.Fatalf("actual initial-root publisher failed: %v", err)
					}
				case <-ctx.Done():
					t.Fatal("actual initial-root publisher did not finish", ctx.Err())
				}
				if !fixture.node.managedGraphInitialRootComplete() {
					t.Fatal("publisher returned without authenticating and marking the initial root complete")
				}
				return nil
			},
		},
		{
			name: "caller-cancelled-at-boundary", wantCancel: true,
			complete: func(t *testing.T, fixture *managedGraphAppendOnlyAdmissionFixture, _ context.Context) <-chan error {
				t.Helper()
				fixture.node.markManagedGraphInitialRootComplete()
				return nil
			},
		},
		{
			name: "node-close-at-boundary", wantCancel: true,
			complete: func(t *testing.T, fixture *managedGraphAppendOnlyAdmissionFixture, ctx context.Context) <-chan error {
				t.Helper()
				binding := fixture.node.internalApplication.Load()
				closed := make(chan error, 1)
				go func() { closed <- fixture.node.Shutdown() }()
				select {
				case <-binding.lifetime.Done():
				case <-ctx.Done():
					t.Fatal("Node.Close did not cancel the borrowed local-read binding", ctx.Err())
				}
				fixture.node.markManagedGraphInitialRootComplete()
				return closed
			},
		},
		{
			name: "binding-replaced-at-boundary", wantCancel: true,
			complete: func(t *testing.T, fixture *managedGraphAppendOnlyAdmissionFixture, _ context.Context) <-chan error {
				t.Helper()
				fixture.node.stopInternalApplication()
				fixture.node.markManagedGraphInitialRootComplete()
				return nil
			},
		},
		{
			name: "voter-authority-lost-at-boundary",
			complete: func(t *testing.T, fixture *managedGraphAppendOnlyAdmissionFixture, _ context.Context) <-chan error {
				t.Helper()
				fixture.node.markManagedGraphInitialRootComplete()
				return nil
			},
		},
		{
			name: "projection-closed-at-boundary",
			complete: func(t *testing.T, fixture *managedGraphAppendOnlyAdmissionFixture, _ context.Context) <-chan error {
				t.Helper()
				if err := fixture.node.material.Close(); err != nil {
					t.Fatal("close projection for negative case", err)
				}
				fixture.node.markManagedGraphInitialRootComplete()
				return nil
			},
		},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			fixture := newManagedGraphAppendOnlyAdmissionFixture(t)
			node := fixture.node
			node.managedGraphInitialRootDone = make(chan struct{})
			ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
			defer cancel()
			callerCtx := ctx
			var cancelCaller context.CancelFunc
			if test.name == "caller-cancelled-at-boundary" {
				callerCtx, cancelCaller = context.WithCancel(ctx)
				defer cancelCaller()
			}
			checked, resume := make(chan struct{}), make(chan struct{})
			var hookOnce sync.Once
			restore := localtesthooks.Set(func(name string) {
				if name == "node:managed-graph:local-read-not-ready" {
					hookOnce.Do(func() {
						close(checked)
						<-resume
					})
				}
			})
			defer restore()
			var resumeOnce sync.Once
			release := func() { resumeOnce.Do(func() { close(resume) }) }
			t.Cleanup(release)
			result := make(chan error, 1)
			go func() {
				_, err := node.GraphQueryManaged(callerCtx, network.GraphQueryRequest{Cypher: "RETURN 1", Consistency: "local"})
				result <- err
			}()
			select {
			case <-checked:
			case <-ctx.Done():
				t.Fatal("local read did not observe the actual incomplete initial-root gate")
			}
			if test.name == "caller-cancelled-at-boundary" {
				cancelCaller()
			}
			closeDone := test.complete(t, fixture, ctx)
			if test.name == "voter-authority-lost-at-boundary" {
				replaceReadCoreWithLearner(t, fixture)
			}
			release()
			select {
			case err := <-result:
				if test.name == "actual-publisher-completes-root" {
					if err != nil {
						t.Fatalf("local Graph read retained stale not-ready after actual root publication: %v", err)
					}
				} else if test.wantCancel {
					if err == nil {
						t.Fatal("local read succeeded after its caller/binding was canceled")
					}
					if test.name == "caller-cancelled-at-boundary" && !errors.Is(err, context.Canceled) {
						t.Fatalf("local read lost caller cancellation: %v", err)
					}
				} else if err == nil {
					t.Fatalf("local Graph read bypassed the %s guard", test.name)
				}
			case <-ctx.Done():
				t.Fatal("local Graph read did not settle at the root-completion boundary", ctx.Err())
			}
			if test.name == "node-close-at-boundary" {
				select {
				case closeErr := <-closeDone:
					if closeErr != nil {
						t.Fatal("Node.Close failed", closeErr)
					}
				case <-ctx.Done():
					t.Fatal("Node.Close did not drain the local read", ctx.Err())
				}
			}
			if test.name == "actual-publisher-completes-root" && (node.core.Tip() != 9 || node.core.RecorderTip() != 9 || node.material.Tip() != 9) {
				t.Fatalf("local read or publisher used unexpected state: C=%d R=%d S=%d", node.core.Tip(), node.core.RecorderTip(), node.material.Tip())
			}
		})
	}
}

func replaceReadCoreWithLearner(t *testing.T, fixture *managedGraphAppendOnlyAdmissionFixture) {
	t.Helper()
	node := fixture.node
	previous := node.core
	protocol := node.managedStartup.Identity.ApplicationProtocol
	node.config.NodeID = "n4"
	learnerMember := peerMember(string(node.config.ClusterID), "n4", "n4-token")
	learnerIdentity := make([]byte, 32)
	for i := range learnerIdentity {
		learnerIdentity[i] = byte(0x70 + i)
	}
	learnerMember.WALIdentity = hex.EncodeToString(learnerIdentity)
	node.config.Learner = &learnerMember
	physical, memberDigest, err := node.preparedBindingDigestsForProtocol(protocol)
	if err != nil {
		t.Fatalf("prepare learner source binding: %v", err)
	}
	bootstrap, err := node.ManagedBootstrapPlanDigestForProtocol(physical, protocol, protocol)
	if err != nil {
		t.Fatalf("prepare learner bootstrap digest: %v", err)
	}
	startupIdentity := node.managedStartup.Identity
	startupIdentity.Member.NodeID = string(learnerMember.ID)
	startupIdentity.Member.SourceBinding = physical
	startupIdentity.Member.ConfigDigest = memberDigest
	startupIdentity.PhysicalBinding = physical
	startupIdentity.BootstrapPlanDigest = bootstrap
	node.managedStartup.Identity = startupIdentity
	walIdentityBytes, err := hex.DecodeString(learnerMember.WALIdentity)
	if err != nil || len(walIdentityBytes) != 32 {
		t.Fatalf("decode learner WAL identity: %v", err)
	}
	wal, err := qlog.Open(filepath.Join(t.TempDir(), "nonvoter-wal"))
	if err != nil {
		t.Fatal(err)
	}
	if err := wal.BindIdentity(walIdentityBytes); err != nil {
		_ = wal.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = wal.Close() })
	var walIdentity [32]byte
	copy(walIdentity[:], walIdentityBytes)
	learner, err := quepaxa.NewLearner(quepaxa.Config{
		NodeID: node.config.NodeID, Cluster: *node.loadClusterConfig(), WAL: wal, Transport: fixture.transport,
		ValidateValue: types.ValidateManagedGraphExecutionPolicy,
		ManagedAppendOnly: &quepaxa.ManagedAppendOnlyConfig{
			Binding: quepaxa.ManagedAppendOnlyBinding{
				ApplicationProtocol: protocol, BootstrapPlanDigest: bootstrap,
				MemberConfigDigest: memberDigest, Generation: startupIdentity.Member.Generation, WALIdentity: walIdentity,
			},
			Verify: node.verifyManagedAppendOnlyCandidate,
		},
	})
	if err != nil {
		t.Fatalf("create actual nonvoter Core: %v", err)
	}
	values := make([]quepaxa.DecidedValue, 0, previous.Tip())
	for slot := quepaxa.Slot(1); slot <= previous.Tip(); slot++ {
		value, ok := previous.CertifiedValue(slot)
		if !ok {
			t.Fatalf("existing certified prefix omitted slot %d", slot)
		}
		values = append(values, value)
	}
	if err := learner.AcceptCertifiedValues(values); err != nil {
		t.Fatalf("restore actual certified prefix into nonvoter Core: %v", err)
	}
	if learner.IsVoter() || learner.Tip() != previous.Tip() {
		t.Fatalf("replacement Core role/progress differs: voter=%t tip=%d want=%d", learner.IsVoter(), learner.Tip(), previous.Tip())
	}
	binding := node.internalApplication.Load()
	binding.mu.Lock()
	binding.core, binding.walStore, binding.wal, binding.member, binding.identity = learner, wal, learnerMember.WALIdentity, learnerMember, startupIdentity
	binding.mu.Unlock()
	node.core, node.wal = learner, wal
}

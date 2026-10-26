package operator

import (
	"bytes"
	"context"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/pkg/fencing"
	"github.com/thanos-io/objstore"
)

// registrationTransferCASBarrier pauses only the root value that revokes the
// predecessor for targetRunID. Runtime-producer preparation and unrelated root
// writes therefore complete normally before the registration CAS is held.
type registrationTransferCASBarrier struct {
	objstore.Bucket
	targetRunID string
	started     chan struct{}
	release     chan struct{}
	startOnce   sync.Once
	releaseOnce sync.Once
}

func (b *registrationTransferCASBarrier) Upload(ctx context.Context, name string, reader io.Reader, options ...objstore.ObjectUploadOption) error {
	body, err := io.ReadAll(reader)
	if err != nil {
		return err
	}
	marker := []byte(`"registrationSupersededBy":"` + b.targetRunID + `"`)
	if strings.Contains(name, "/rhiza-authority/v1/roots/") && bytes.Contains(body, marker) {
		b.startOnce.Do(func() { close(b.started) })
		select {
		case <-b.release:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return b.Bucket.Upload(ctx, name, bytes.NewReader(body), options...)
}

func (b *registrationTransferCASBarrier) unblock() {
	b.releaseOnce.Do(func() { close(b.release) })
}

type registrationCASHarness struct {
	f       supervisorRegistrationFixture
	client  fencing.AuthorityClient
	prior   fencing.Identity
	runtime fencing.RuntimeBinding
	wal     string
	gate    *registrationTransferCASBarrier
}

func newRegistrationCASHarness(t *testing.T, transferRunID string) registrationCASHarness {
	t.Helper()
	f := newSupervisorRegistrationFixture(t, true)
	prepareInitialRegistrationFixture(t, &f)
	prior := f.identity
	client := fencing.AuthorityClient{URL: f.server.URL + "/v1/supervisor", TokenFile: f.token, Client: f.server.Client()}
	if _, err := client.RegisterInitialStartup(t.Context(), prior); err != nil {
		t.Fatalf("register initial predecessor: %v", err)
	}
	root, _, err := f.state.ReadRoot(t.Context(), "sts-uid", prior.SourceClusterID)
	if err != nil {
		t.Fatal(err)
	}
	run := root.Ledger.Runs[prior.RunID]
	if run.Runtime == nil || run.MutationProducerUID == "" || run.Identity.WALIdentity != "" {
		t.Fatalf("predecessor setup lacks its pinned zero-WAL producer: %+v", run)
	}
	gate := &registrationTransferCASBarrier{Bucket: f.e.AuthorityState.bucket, targetRunID: transferRunID,
		started: make(chan struct{}), release: make(chan struct{})}
	wrapped, err := NewAuthorityStateStore(gate, f.e.AuthorityState.prefix)
	if err != nil {
		t.Fatal(err)
	}
	f.e.AuthorityState = wrapped
	t.Cleanup(gate.unblock)
	return registrationCASHarness{f: f, client: client, prior: prior, runtime: *run.Runtime,
		wal: strings.Repeat("a", 64), gate: gate}
}

func (h registrationCASHarness) runTransfer(t *testing.T, runID string) <-chan error {
	t.Helper()
	next := h.prior
	next.RunID = runID
	result := make(chan error, 1)
	go func() {
		_, err := h.client.RegisterStartupContinuation(t.Context(), next, h.wal)
		result <- err
	}()
	select {
	case <-h.gate.started:
		return result
	case <-time.After(5 * time.Second):
		h.gate.unblock()
		t.Fatal("continuation did not reach the prepared predecessor root CAS")
		return result
	}
}

func awaitRegistrationRaceResult(t *testing.T, result <-chan error) error {
	t.Helper()
	select {
	case err := <-result:
		return err
	case <-time.After(5 * time.Second):
		t.Fatal("registration continuation did not finish after root CAS release")
		return context.DeadlineExceeded
	}
}

func assertRegistrationTransferWinner(t *testing.T, h registrationCASHarness, winner string, expectBound bool) {
	t.Helper()
	root, _, err := h.f.state.ReadRoot(t.Context(), "sts-uid", h.prior.SourceClusterID)
	if err != nil {
		t.Fatal(err)
	}
	prior, ok := root.Ledger.Runs[h.prior.RunID]
	if !ok {
		t.Fatal("canonical predecessor Run disappeared")
	}
	if expectBound {
		want := h.prior
		want.WALIdentity = h.wal
		if prior.Identity != want || prior.RegistrationSupersededBy != "" {
			t.Fatalf("Bind winner root differs from the exact bound predecessor: %+v", prior)
		}
	} else {
		if prior.Identity != h.prior || prior.RegistrationSupersededBy != winner {
			t.Fatalf("transfer winner root differs from exact predecessor revocation: %+v", prior)
		}
		next, ok := root.Ledger.Runs[winner]
		wantNext := h.prior
		wantNext.RunID = winner
		if !ok || next.Identity != wantNext || next.Identity.WALIdentity != "" || next.RegistrationExpectedWAL != h.wal {
			t.Fatalf("transfer winner Run did not pin its expected WAL: %+v exists=%t", next, ok)
		}
	}
	producer, uid, _, err := h.f.state.ReadProducer(t.Context(), h.runtime)
	if err != nil || uid != stableMutationProducerUID(h.runtime) {
		t.Fatalf("predecessor producer identity changed after race: uid=%q err=%v", uid, err)
	}
	if expectBound {
		if producer.WALIdentity != h.wal || producer.Phase == fencing.MutationReserved {
			t.Fatalf("winning Bind did not retain its exact producer transition: %+v", producer)
		}
	} else if producer.WALIdentity != "" || producer.Phase != fencing.MutationReserved {
		t.Fatalf("losing predecessor producer was changed by continuation: %+v", producer)
	}
	if winner != "" && winner != "closure" {
		winnerRun, ok := root.Ledger.Runs[winner]
		if !ok || winnerRun.Runtime == nil {
			t.Fatalf("winning continuation has no enrolled runtime: %+v exists=%t", winnerRun, ok)
		}
		winnerRuntime := *winnerRun.Runtime
		producer, uid, _, err = h.f.state.ReadProducer(t.Context(), winnerRuntime)
		if err != nil || uid != stableMutationProducerUID(winnerRuntime) || producer.WALIdentity != "" || producer.Phase != fencing.MutationReserved {
			t.Fatalf("winning continuation producer differs from its exact zero-WAL reservation: uid=%q producer=%+v err=%v", uid, producer, err)
		}
	}
}

func TestRegistrationTransferConditionalCASOldBindWins(t *testing.T) {
	const transferRunID = "transfer-loses-to-bind"
	h := newRegistrationCASHarness(t, transferRunID)
	transferResult := h.runTransfer(t, transferRunID)
	prior := h.prior
	prior.WALIdentity = h.wal
	if _, err := h.client.Bind(t.Context(), prior); err != nil {
		t.Fatalf("prior Bind did not win while transfer CAS was held: %v", err)
	}
	h.gate.unblock()
	if err := awaitRegistrationRaceResult(t, transferResult); err == nil {
		t.Fatal("stale continuation succeeded after prior Bind won the root CAS")
	}
	assertRegistrationTransferWinner(t, h, "", true)
	root, _, err := h.f.state.ReadRoot(t.Context(), "sts-uid", h.prior.SourceClusterID)
	if err != nil {
		t.Fatal(err)
	}
	if _, exists := root.Ledger.Runs[transferRunID]; exists {
		t.Fatal("losing continuation left a successor Run behind")
	}
}

func TestRegistrationTransferConditionalCASTransferWinsThenOldBindLoses(t *testing.T) {
	const transferRunID = "transfer-wins-bind-loses"
	h := newRegistrationCASHarness(t, transferRunID)
	transferResult := h.runTransfer(t, transferRunID)
	h.gate.unblock()
	if err := awaitRegistrationRaceResult(t, transferResult); err != nil {
		t.Fatalf("continuation failed after its conditional root CAS won: %v", err)
	}
	prior := h.prior
	prior.WALIdentity = h.wal
	if _, err := h.client.Bind(t.Context(), prior); err == nil {
		t.Fatal("old Run regained Bind after continuation won the root CAS")
	}
	assertRegistrationTransferWinner(t, h, transferRunID, false)
}

func TestRegistrationTransferConditionalCASClosureWins(t *testing.T) {
	const transferRunID = "transfer-loses-to-close"
	h := newRegistrationCASHarness(t, transferRunID)
	transferResult := h.runTransfer(t, transferRunID)
	closeHash := hashBytes([]byte("registration-race-close"))
	closed, err := h.f.e.change(t.Context(), "sts-uid", h.prior.SourceClusterID, func(l *fencing.Ledger) error {
		return l.CloseNodes("registration-race-close", closeHash, []string{h.prior.NodeID})
	})
	if err != nil {
		t.Fatalf("close predecessor cohort while transfer CAS was held: %v", err)
	}
	settled, err := h.f.e.closeMutationProducers(t.Context(), closed, []string{h.prior.NodeID})
	if err != nil || !settled {
		t.Fatalf("settle exact closed predecessor producer: settled=%t err=%v", settled, err)
	}
	h.gate.unblock()
	if err := awaitRegistrationRaceResult(t, transferResult); err == nil {
		t.Fatal("continuation succeeded after closure won the root CAS")
	}
	root, _, err := h.f.state.ReadRoot(t.Context(), "sts-uid", h.prior.SourceClusterID)
	if err != nil {
		t.Fatal(err)
	}
	if root.Ledger.Runs[h.prior.RunID].Stop == nil || root.Ledger.Runs[h.prior.RunID].RegistrationSupersededBy != "" ||
		root.Ledger.ClosedNodes[h.prior.NodeID].OperationID != "registration-race-close" {
		t.Fatalf("closure winner root was changed by losing continuation: %+v", root.Ledger)
	}
	if _, exists := root.Ledger.Runs[transferRunID]; exists {
		t.Fatal("losing continuation left a successor Run after closure")
	}
	producer, uid, _, err := h.f.state.ReadProducer(t.Context(), h.runtime)
	if err != nil || uid != stableMutationProducerUID(h.runtime) || producer.Phase != fencing.MutationClosed || producer.WALIdentity != "" ||
		producer.CloseOperationID != root.Ledger.Runs[h.prior.RunID].Stop.OperationID || producer.CloseRequestHash != root.Ledger.Runs[h.prior.RunID].Stop.RequestHash {
		t.Fatalf("closure winner producer differs from the exact stopped predecessor: uid=%q producer=%+v err=%v", uid, producer, err)
	}
}

func TestRegistrationTransferConditionalCASOtherContinuationWins(t *testing.T) {
	const transferRunID = "transfer-loses-to-other-continuation"
	const winnerRunID = "other-continuation-wins"
	h := newRegistrationCASHarness(t, transferRunID)
	transferResult := h.runTransfer(t, transferRunID)
	winner := h.prior
	winner.RunID = winnerRunID
	if _, err := h.client.RegisterStartupContinuation(t.Context(), winner, h.wal); err != nil {
		t.Fatalf("other continuation failed to win while first CAS was held: %v", err)
	}
	h.gate.unblock()
	if err := awaitRegistrationRaceResult(t, transferResult); err == nil {
		t.Fatal("stale continuation succeeded after another continuation won")
	}
	assertRegistrationTransferWinner(t, h, winnerRunID, false)
	root, _, err := h.f.state.ReadRoot(t.Context(), "sts-uid", h.prior.SourceClusterID)
	if err != nil {
		t.Fatal(err)
	}
	if _, exists := root.Ledger.Runs[transferRunID]; exists {
		t.Fatal("losing continuation left a successor Run behind")
	}
}

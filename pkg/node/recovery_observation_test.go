package node

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/qlog"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

type blockedRecoveryReadTransport struct{}

func (blockedRecoveryReadTransport) SendRecord(context.Context, quepaxa.NodeID, quepaxa.RecordRequest) (quepaxa.Summary, error) {
	return quepaxa.Summary{}, errors.New("unexpected record")
}
func (blockedRecoveryReadTransport) SendDecision(context.Context, quepaxa.Decision) error {
	return errors.New("unexpected decision")
}
func (blockedRecoveryReadTransport) ReadTip(ctx context.Context, _ quepaxa.NodeID) (quepaxa.Slot, error) {
	<-ctx.Done()
	return 0, ctx.Err()
}

type failedRecoveryReadTransport struct{}

func (failedRecoveryReadTransport) SendRecord(context.Context, quepaxa.NodeID, quepaxa.RecordRequest) (quepaxa.Summary, error) {
	return quepaxa.Summary{}, errors.New("unexpected record")
}
func (failedRecoveryReadTransport) SendDecision(context.Context, quepaxa.Decision) error {
	return errors.New("unexpected decision")
}
func (failedRecoveryReadTransport) ReadTip(context.Context, quepaxa.NodeID) (quepaxa.Slot, error) {
	return 0, errors.New("peer unavailable")
}
func (failedRecoveryReadTransport) StageValue(context.Context, quepaxa.NodeID, quepaxa.ValueHash, []byte) error {
	return errors.New("unexpected stage")
}
func (failedRecoveryReadTransport) FetchValue(context.Context, quepaxa.NodeID, quepaxa.ValueHash) ([]byte, error) {
	return nil, errors.New("unexpected fetch")
}
func (blockedRecoveryReadTransport) StageValue(context.Context, quepaxa.NodeID, quepaxa.ValueHash, []byte) error {
	return errors.New("unexpected stage")
}
func (blockedRecoveryReadTransport) FetchValue(context.Context, quepaxa.NodeID, quepaxa.ValueHash) ([]byte, error) {
	return nil, errors.New("unexpected fetch")
}

type exclusionRecoveryReadTransport struct {
	cores map[quepaxa.NodeID]*quepaxa.Core
	down  quepaxa.NodeID
}

func (exclusionRecoveryReadTransport) SendRecord(context.Context, quepaxa.NodeID, quepaxa.RecordRequest) (quepaxa.Summary, error) {
	return quepaxa.Summary{}, errors.New("unexpected record")
}
func (exclusionRecoveryReadTransport) SendDecision(context.Context, quepaxa.Decision) error {
	return errors.New("unexpected decision")
}
func (transport exclusionRecoveryReadTransport) ReadTip(_ context.Context, id quepaxa.NodeID) (quepaxa.Slot, error) {
	if id == transport.down {
		return 0, errors.New("replica unavailable")
	}
	return transport.cores[id].Tip(), nil
}
func (exclusionRecoveryReadTransport) StageValue(context.Context, quepaxa.NodeID, quepaxa.ValueHash, []byte) error {
	return errors.New("unexpected stage")
}
func (exclusionRecoveryReadTransport) FetchValue(context.Context, quepaxa.NodeID, quepaxa.ValueHash) ([]byte, error) {
	return nil, errors.New("unexpected fetch")
}

func recoveryObservationNode(t *testing.T, nodeID quepaxa.NodeID, members []quepaxa.Member, learner bool) *Node {
	t.Helper()
	wal, err := qlog.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = wal.Close() })
	config := quepaxa.Config{
		NodeID:  nodeID,
		Cluster: quepaxa.Cluster{ConfigID: 7, Members: members},
		WAL:     wal,
	}
	var core *quepaxa.Core
	if learner {
		core, err = quepaxa.NewLearner(config)
	} else {
		core, err = quepaxa.New(config)
	}
	if err != nil {
		t.Fatal(err)
	}
	return &Node{
		config: &types.ExecutionConfig{NodeID: types.NodeID(nodeID), ClusterID: "cluster-a", ObjStoreDurability: types.ObjectStoreDurabilityBeforeAck},
		core:   core,
	}
}

func TestVoterRecoveryObservationSeparatesLocalReadyAndQuorum(t *testing.T) {
	n := recoveryObservationNode(t, "n1", []quepaxa.Member{{ID: "n1"}}, false)
	status := n.voterRecoveryStatus(context.Background(), "")
	if status.Ready || !status.Voting || !status.Quorum || !status.QuorumKnown || status.QuorumConfigID != status.ConfigID {
		t.Fatalf("initial status=%+v", status)
	}
	if status.ConfigID != 7 || len(status.Voters) != 1 || status.Voters[0] != "n1" {
		t.Fatalf("membership status=%+v", status)
	}
	n.ready.Store(true)
	status = n.voterRecoveryStatus(context.Background(), "")
	if !status.Ready || !status.Voting || !status.QuorumKnown {
		t.Fatalf("ready status=%+v", status)
	}
}

func TestLearnerCanBeLocallyReadyWithoutVoterQuorum(t *testing.T) {
	n := recoveryObservationNode(t, "learner", []quepaxa.Member{{ID: "n1"}}, true)
	n.ready.Store(true)
	status := n.voterRecoveryStatus(context.Background(), "")
	if !status.Ready || status.Voting || status.Quorum || status.QuorumKnown || status.QuorumUnavailable {
		t.Fatalf("learner status=%+v", status)
	}
	if status.WALIdentity == "" {
		t.Fatal("learner WAL identity missing")
	}
}

func TestExcludedSelfIsUnknownAndAbsentExclusionUsesCurrentConfig(t *testing.T) {
	n := recoveryObservationNode(t, "n1", []quepaxa.Member{{ID: "n1"}}, false)
	status := n.voterRecoveryStatus(context.Background(), "n1")
	if status.Quorum || status.QuorumKnown || status.QuorumUnavailable || status.QuorumConfigID != 0 || status.ReadIndexSlot != 0 {
		t.Fatalf("self-excluded status is not unknown: %+v", status)
	}
	status = n.voterRecoveryStatus(context.Background(), "removed-voter")
	if !status.Quorum || !status.QuorumKnown || status.QuorumUnavailable || status.QuorumConfigID != status.ConfigID {
		t.Fatalf("absent voter exclusion failed against current config: %+v", status)
	}
}

func TestExcludingActiveVoterDoesNotReuseNormalQuorum(t *testing.T) {
	members := []quepaxa.Member{{ID: "n1"}, {ID: "n2"}, {ID: "n3"}}
	cluster := quepaxa.Cluster{ConfigID: 7, Members: members}
	transport := &exclusionRecoveryReadTransport{cores: make(map[quepaxa.NodeID]*quepaxa.Core), down: "n3"}
	wals := make([]*qlog.WAL, 0, len(members))
	for _, member := range members {
		wal, err := qlog.Open(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		wals = append(wals, wal)
		core, err := quepaxa.New(quepaxa.Config{NodeID: member.ID, Cluster: cluster, WAL: wal, Transport: transport})
		if err != nil {
			t.Fatal(err)
		}
		transport.cores[member.ID] = core
	}
	for _, wal := range wals {
		t.Cleanup(func() { _ = wal.Close() })
	}
	n := &Node{
		config: &types.ExecutionConfig{NodeID: "n1", ClusterID: "cluster-a"},
		core:   transport.cores["n1"],
	}
	n.ready.Store(true)

	ordinary := n.voterRecoveryStatus(context.Background(), "")
	if !ordinary.Quorum || !ordinary.QuorumKnown || ordinary.QuorumUnavailable || ordinary.QuorumConfigID != ordinary.ConfigID {
		t.Fatalf("ordinary n1+n2 quorum not confirmed: %+v", ordinary)
	}
	excluded := n.voterRecoveryStatus(context.Background(), "n2")
	if excluded.Quorum || excluded.QuorumKnown || !excluded.QuorumUnavailable || excluded.QuorumConfigID != 0 || excluded.ReadIndexSlot != 0 {
		t.Fatalf("excluding n2 reused normal quorum evidence: %+v", excluded)
	}
}

func TestConfirmedRecoveryReadIndexContextTakesPrecedence(t *testing.T) {
	before := recoveryMembershipSnapshot{
		configID: 7, voters: []string{"n1", "n2"}, walIdentity: "wal-n1", voting: true,
	}
	revision := recoveryMembershipRevision{configID: 7}
	for _, tc := range []struct {
		name   string
		cancel bool
		err    error
	}{
		{name: "success after cancellation", cancel: true},
		{name: "typed error tied with cancellation", cancel: true, err: quepaxa.ErrQuorumUnavailable},
		{name: "success after deadline", err: nil},
		{name: "typed error tied with deadline", err: quepaxa.ErrQuorumUnavailable},
		{name: "typed error wraps deadline", err: errors.Join(quepaxa.ErrQuorumUnavailable, context.DeadlineExceeded)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var ctx context.Context
			var cancel context.CancelFunc
			if tc.cancel {
				ctx, cancel = context.WithCancel(context.Background())
			} else {
				ctx, cancel = context.WithDeadline(context.Background(), time.Now().Add(-time.Millisecond))
			}
			defer cancel()
			afterCalled := false
			_, known, unavailable := confirmedRecoveryReadIndex(ctx, before, revision, "n1", func() recoveryMembershipSnapshot {
				afterCalled = true
				return before
			}, func() recoveryMembershipRevision { return revision }, func(context.Context) (quepaxa.Slot, error) {
				if tc.cancel {
					cancel()
				}
				return 0, tc.err
			})
			if known || unavailable || !afterCalled {
				t.Fatalf("canceled context classified as evidence or skipped after snapshot: known=%t unavailable=%t after=%t", known, unavailable, afterCalled)
			}
		})
	}
}

func TestRecoveryReadIndexRequiresOuterRevisionAgreement(t *testing.T) {
	before := recoveryMembershipSnapshot{configID: 7, voters: []string{"n1", "n2"}, walIdentity: "wal-n1", voting: true}
	revision := recoveryMembershipRevision{configID: 7}
	for _, tc := range []struct {
		name          string
		after         recoveryMembershipSnapshot
		afterRevision recoveryMembershipRevision
		wantEvidence  bool
	}{
		{name: "stable", after: before, afterRevision: revision, wantEvidence: true},
		{name: "terminal after tuple getter", after: before, afterRevision: recoveryMembershipRevision{configID: 8}},
		{name: "abort ABA after tuple getter", after: before, afterRevision: recoveryMembershipRevision{configID: 7, abortSlot: 11}},
		{name: "self WAL changed", after: recoveryMembershipSnapshot{configID: 7, voters: []string{"n1", "n2"}, walIdentity: "wal-new", voting: true}, afterRevision: revision},
		{name: "became pending", after: recoveryMembershipSnapshot{configID: 7, voters: []string{"n1", "n2"}, walIdentity: "wal-n1", voting: true, pending: true}, afterRevision: revision},
		{name: "self stopped voting", after: recoveryMembershipSnapshot{configID: 7, voters: []string{"n1", "n2"}, walIdentity: "wal-n1"}, afterRevision: revision},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reads, afters := 0, 0
			_, known, unavailable := confirmedRecoveryReadIndex(context.Background(), before, revision, "n1",
				func() recoveryMembershipSnapshot { afters++; return tc.after },
				func() recoveryMembershipRevision { return tc.afterRevision },
				func(context.Context) (quepaxa.Slot, error) { reads++; return 0, quepaxa.ErrQuorumUnavailable },
			)
			if reads != 1 || afters != 1 {
				t.Fatalf("read/after counts=%d/%d, want exactly one", reads, afters)
			}
			if known || unavailable != tc.wantEvidence {
				t.Fatalf("known=%t unavailable=%t, want unavailable=%t", known, unavailable, tc.wantEvidence)
			}
		})
	}

	reads := 0
	_, known, unavailable := confirmedRecoveryReadIndex(context.Background(), before,
		recoveryMembershipRevision{configID: 6}, "n1",
		func() recoveryMembershipSnapshot {
			t.Fatal("after snapshot called without eligible before snapshot")
			return before
		},
		func() recoveryMembershipRevision { return revision },
		func(context.Context) (quepaxa.Slot, error) { reads++; return 0, nil },
	)
	if reads != 0 || known || unavailable {
		t.Fatalf("inconsistent before revision accepted: reads=%d known=%t unavailable=%t", reads, known, unavailable)
	}
}

func TestReadIndexTimeoutIsUnknownNotNoQuorumProof(t *testing.T) {
	wal, err := qlog.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = wal.Close() })
	members := []quepaxa.Member{{ID: "n1"}, {ID: "n2"}, {ID: "n3"}}
	core, err := quepaxa.New(quepaxa.Config{
		NodeID: "n1", Cluster: quepaxa.Cluster{ConfigID: 7, Members: members}, WAL: wal,
		Transport: blockedRecoveryReadTransport{},
	})
	if err != nil {
		t.Fatal(err)
	}
	n := &Node{config: &types.ExecutionConfig{NodeID: "n1", ClusterID: "cluster-a"}, core: core}
	n.ready.Store(true)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	status := n.voterRecoveryStatus(ctx, "")
	if !status.Ready || !status.Voting || status.Quorum || status.QuorumKnown || status.QuorumUnavailable {
		t.Fatalf("timed-out status=%+v", status)
	}
}

func TestTypedQuorumUnavailableHasNoSuccessMetadata(t *testing.T) {
	wal, err := qlog.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = wal.Close() })
	core, err := quepaxa.New(quepaxa.Config{
		NodeID: "n1", Cluster: quepaxa.Cluster{ConfigID: 7, Members: []quepaxa.Member{{ID: "n1"}, {ID: "n2"}, {ID: "n3"}}},
		WAL: wal, Transport: failedRecoveryReadTransport{},
	})
	if err != nil {
		t.Fatal(err)
	}
	n := &Node{config: &types.ExecutionConfig{NodeID: "n1", ClusterID: "cluster-a"}, core: core}
	status := n.voterRecoveryStatus(context.Background(), "")
	if !status.QuorumUnavailable || status.QuorumKnown || status.Quorum || status.QuorumConfigID != 0 || status.ReadIndexSlot != 0 {
		t.Fatalf("failed ReadIndex carried success metadata: %+v", status)
	}
}

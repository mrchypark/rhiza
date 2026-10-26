package node

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"io"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/mrchypark/rhiza/pkg/recovery"
	"github.com/thanos-io/objstore"
)

type failArchiveHeadUploadsBucket struct {
	objstore.Bucket
	enabled atomic.Bool
	failed  atomic.Bool
}

func (b *failArchiveHeadUploadsBucket) Upload(ctx context.Context, name string, reader io.Reader, options ...objstore.ObjectUploadOption) error {
	if b.enabled.Load() && strings.HasSuffix(name, "/archive/head.bin") {
		b.failed.Store(true)
		return errors.New("injected archive head persistence failure")
	}
	return b.Bucket.Upload(ctx, name, reader, options...)
}

func TestMembershipRetryDurablyPublishesCommittedConfiguration(t *testing.T) {
	ctx := context.Background()
	n, identities, bucket, transport := newMembershipOperationNode(t)
	transport.disable("c")
	if _, _, err := n.core.Propose(ctx, []byte("membership durability baseline")); err != nil {
		t.Fatalf("commit preexisting archive baseline: %v", err)
	}
	baselineTip := n.core.Tip()
	if _, ok := n.core.CertifiedValue(baselineTip); !ok {
		t.Fatalf("baseline slot %d is not certified", baselineTip)
	}
	if err := n.core.EnsureDurableThrough(ctx, baselineTip); err != nil {
		t.Fatalf("make preexisting archive baseline locally durable: %v", err)
	}
	if err := n.archive.SyncThrough(ctx, n.core, baselineTip); err != nil {
		t.Fatalf("publish preexisting archive baseline: %v", err)
	}
	if got := n.archive.Tip(); got != baselineTip {
		t.Fatalf("preexisting archive tip=%d, want certified baseline %d", got, baselineTip)
	}

	faults := &failArchiveHeadUploadsBucket{Bucket: bucket}
	faults.enabled.Store(true)
	n.archive.Close()
	n.archive = recovery.NewManager(faults, "cluster", 1)
	if err := n.archive.Load(ctx); err != nil {
		t.Fatalf("load archive before mutation: %v", err)
	}
	if got := n.archive.Tip(); got != baselineTip {
		t.Fatalf("fault-wrapped archive tip=%d, want existing baseline %d", got, baselineTip)
	}
	t.Cleanup(n.archive.Close)

	request := network.MembershipChange{
		OperationID: "durable-remove-c", ClusterID: "cluster", ExpectedConfigID: 1, Remove: "c",
		Fence: &network.MembershipFence{NodeID: "c", WALIdentity: identities["c"], WorkloadUID: "workload-c", Confirmed: true, Evidence: "external-fence"},
	}
	if err := n.changeMembership(ctx, request); err == nil {
		t.Fatal("membership request succeeded despite injected post-commit archive failure")
	}
	if !faults.failed.Load() {
		t.Fatal("membership request failed before the injected archive head upload failure")
	}
	committed := n.core.CurrentCluster()
	if committed.ConfigID != 2 || len(committed.Members) != 2 || containsMember(committed.Members, "c") {
		t.Fatalf("configuration did not commit before persistence failure: %+v", committed)
	}
	changedPayload := request
	changedFence := *request.Fence
	changedFence.Evidence = "different-fence"
	changedPayload.Fence = &changedFence
	journalKey := n.membershipOperationKey(request)
	journalData, err := readVoterRegistration(ctx, n.bucket, journalKey)
	if err != nil || journalData == nil {
		t.Fatalf("post-commit membership journal key=%q missing after failed persistence: present=%t err=%v", journalKey, journalData != nil, err)
	}
	var journal membershipOperation
	if err := json.Unmarshal(journalData, &journal); err != nil {
		t.Fatalf("post-commit membership journal key=%q sha256=%x is invalid: %v", journalKey, sha256.Sum256(journalData), err)
	}
	if !reflect.DeepEqual(journal.Request, request) || !reflect.DeepEqual(journal.Target, committed) {
		t.Fatalf("post-commit membership journal key=%q sha256=%x does not match exact request/committed target", journalKey, sha256.Sum256(journalData))
	}
	if reflect.DeepEqual(journal.Request, changedPayload) {
		t.Fatalf("changed replay unexpectedly matches persisted request at key=%q sha256=%x", journalKey, sha256.Sum256(journalData))
	}
	through := n.core.Tip()
	if through <= baselineTip {
		t.Fatalf("membership decision did not advance beyond published archive baseline: baseline=%d tip=%d", baselineTip, through)
	}
	terminal, ok := n.core.CertifiedValue(through)
	if !ok {
		t.Fatalf("membership terminal slot %d is not certified", through)
	}
	control, err := quepaxa.DecodeReconfiguration(terminal.Value)
	if err != nil || !control {
		t.Fatalf("slot %d is not the certified membership terminal: control=%t err=%v", through, control, err)
	}
	if got := n.archive.Tip(); got != baselineTip {
		t.Fatalf("archive tip=%d advanced past failed membership publication baseline %d", got, baselineTip)
	}
	if err := n.core.EnsureDurableThrough(ctx, through); err != nil {
		t.Fatalf("committed decision was not durable in the local WAL: %v", err)
	}
	if got := n.archive.Tip(); got >= through {
		t.Fatalf("archive tip=%d unexpectedly covers failed publication target %d", got, through)
	}

	// Conflicting payloads and operation IDs must not adopt the committed
	// configuration or publish a different request as its acknowledgement.
	if err := n.changeMembership(ctx, changedPayload); !errors.Is(err, network.ErrRequestConflict) {
		t.Fatalf("changed payload replay error=%v, want request conflict", err)
	}
	if changedPayload.Fence.Evidence != "different-fence" || request.Fence.Evidence != "external-fence" {
		t.Fatalf("changed payload replay mutated caller fence: changed=%q original=%q", changedPayload.Fence.Evidence, request.Fence.Evidence)
	}
	changedOperation := request
	changedOperation.OperationID += "-other"
	if err := n.changeMembership(ctx, changedOperation); !errors.Is(err, network.ErrRequestConflict) {
		t.Fatalf("changed operation replay error=%v, want request conflict", err)
	}
	changedConfig := request
	changedConfig.ExpectedConfigID = 2
	if err := n.changeMembership(ctx, changedConfig); !errors.Is(err, network.ErrInvalidRequest) {
		t.Fatalf("stale removed-member request at next config error=%v, want invalid request", err)
	}
	if got := n.core.Tip(); got != through {
		t.Fatalf("rejected replay changed consensus tip from %d to %d", through, got)
	}
	assertNoMembershipOperation(t, ctx, n, bucket, 2)

	// Only the exact journaled request may retry persistence. It must not
	// commit another config decision or advance the consensus tip.
	faults.enabled.Store(false)
	if err := n.changeMembership(ctx, request); err != nil {
		t.Fatalf("persist committed configuration on exact replay: %v", err)
	}
	if got := n.core.Tip(); got != through {
		t.Fatalf("exact persistence replay advanced tip from %d to %d", through, got)
	}
	final := n.core.CurrentCluster()
	if final.ConfigID != 2 || len(final.Members) != 2 || containsMember(final.Members, "c") {
		t.Fatalf("exact replay changed committed configuration: %+v", final)
	}
	assertDurableArchivedMembership(t, ctx, n, bucket, through)

	terminal, ok = n.core.CertifiedValue(through)
	if !ok {
		t.Fatalf("committed terminal slot %d is not certified", through)
	}
	reloaded := recovery.NewManager(bucket, "cluster", 1)
	defer reloaded.Close()
	if err := reloaded.Load(ctx); err != nil {
		t.Fatalf("reload persisted membership archive: %v", err)
	}
	values, _, err := reloaded.DecisionsFrom(ctx, through, 1)
	if err != nil || len(values) != 1 || values[0].Hash != terminal.Hash || string(values[0].Value) != string(terminal.Value) {
		t.Fatalf("reloaded terminal differs from committed membership decision: values=%+v err=%v", values, err)
	}
}

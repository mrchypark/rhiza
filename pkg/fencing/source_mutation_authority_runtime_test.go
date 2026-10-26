package fencing

import (
	"strings"
	"sync"
	"testing"
)

func logicalAuthorityForRetryTest(receipt BindReceipt) *AuthorityClient {
	copy := receipt.clone()
	producer := *receipt.Producer
	if producer.LogicalBinding != nil {
		logical := *producer.LogicalBinding
		producer.LogicalBinding = &logical
	}
	return &AuthorityClient{AuthorityProfile: receipt.AuthorityProfile, boundIdentity: receipt.Identity,
		producer: &producer, binding: &copy, role: StartupRoleVoterActive}
}

func logicalRetryReceiptForTest() BindReceipt {
	id := Identity{SourceClusterID: "source", NodeID: "node-1", PodName: "rhiza-0", PodUID: "pod-uid", RunID: "run-1", WALIdentity: strings.Repeat("c", 64)}
	binding := LogicalWriterBindingV1{Version: LogicalWriterVersionV1, Profile: LogicalWriterProfileV1,
		SourceClusterID: id.SourceClusterID, NodeID: id.NodeID, RunID: id.RunID, WALIdentity: id.WALIdentity,
		ProducerUID: "logical-producer", StoreScopeHash: journalTestDigest, Role: StartupRoleVoterActive}
	producer := &ProducerBinding{ContractVersion: MutationIntentVersion, Profile: LogicalWriterProfileV1,
		LogicalBinding: &binding, ProducerUID: binding.ProducerUID, StoreScopeHash: binding.StoreScopeHash,
		ExecutionProfile: MutationExecutionProfile, StageIncarnation: strings.Repeat("b", 64)}
	return BindReceipt{Identity: id, Role: StartupRoleVoterActive, AuthorityProfile: LogicalWriterProfileV1,
		LogicalBinding: &binding, Producer: producer}
}

func TestAuthoritySourceMutationClientCreatesAndReopensExactLogicalRetryState(t *testing.T) {
	receipt := logicalRetryReceiptForTest()
	authority := logicalAuthorityForRetryTest(receipt)
	dir := t.TempDir() + "/run-retry"

	created, err := CreateAuthoritySourceMutationClient(dir, authority, receipt)
	if err != nil {
		t.Fatal(err)
	}
	intent := sourceRetryIntent("first-upload")
	intent.StoreScopeHash = receipt.Producer.StoreScopeHash
	stageSourceRetryTest(t, created.state, intent)
	step, err := created.state.allocate(intent)
	if err != nil {
		t.Fatal(err)
	}
	if step.Sequence != 1 || step.Entry.Intent.OperationID != MutationRequestID(receipt.Producer.ProducerUID, 1) {
		t.Fatalf("unexpected persisted original retry tuple: %+v", step)
	}
	if err := created.state.Close(); err != nil {
		t.Fatal(err)
	}
	created.closed = true

	reopened, err := OpenAuthoritySourceMutationClient(dir, authority, receipt)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if len(reopened.state.data.Steps) != 1 || reopened.state.data.Steps[0].Entry.Intent != step.Entry.Intent ||
		reopened.state.data.NextSequence != 2 || reopened.client.authority != authority {
		t.Fatal("direct-authority opener did not recover the original tuple through the shared mutation client")
	}
	if _, err := OpenAuthoritySourceMutationClient(dir+"/missing", authority, receipt); err == nil {
		t.Fatal("direct-authority opener created a missing retry journal")
	}
	changed := receipt.clone()
	changed.Producer.ProducerUID = "different-producer"
	if _, err := OpenAuthoritySourceMutationClient(dir, authority, changed); err == nil {
		t.Fatal("direct-authority opener accepted a receipt different from its validated Bind")
	}
	changed = receipt.clone()
	changed.Identity.RunID = "another-run"
	if _, err := OpenAuthoritySourceMutationClient(dir, authority, changed); err == nil {
		t.Fatal("direct-authority opener accepted another bound Identity")
	}
	authority.AuthorityProfile = RuntimeWriterProfileV1
	if _, err := OpenAuthoritySourceMutationClient(dir, authority, receipt); err == nil {
		t.Fatal("direct-authority opener accepted a profile changed after Bind")
	}
}

func TestAuthoritySourceMutationConcurrentOpenHasSingleRetryOwner(t *testing.T) {
	receipt := logicalRetryReceiptForTest()
	authority := logicalAuthorityForRetryTest(receipt)
	dir := t.TempDir() + "/run-retry"
	state, err := createSourceRetryState(dir, receipt)
	if err != nil {
		t.Fatal(err)
	}
	if err := state.Close(); err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	results := make(chan *SourceMutationClient, 2)
	errs := make(chan error, 2)
	for range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			client, err := OpenAuthoritySourceMutationClient(dir, authority, receipt)
			if err != nil {
				errs <- err
				return
			}
			results <- client
		}()
	}
	wg.Wait()
	close(results)
	close(errs)
	var opened []*SourceMutationClient
	for client := range results {
		opened = append(opened, client)
	}
	var failures int
	for range errs {
		failures++
	}
	for _, client := range opened {
		if err := client.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if len(opened) != 1 || failures != 1 {
		t.Fatalf("retry journal had %d owners and %d rejected opens; want exactly one each", len(opened), failures)
	}
}

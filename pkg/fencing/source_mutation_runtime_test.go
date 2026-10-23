package fencing

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"net/http"
	"path/filepath"
	"testing"
)

type relayConditionAuthority struct {
	id        Identity
	producer  *MutationProducer
	operation int
}

func (a *relayConditionAuthority) Register(context.Context, Identity) error { return nil }
func (a *relayConditionAuthority) Bind(_ context.Context, id Identity) (BindReceipt, error) {
	runtime := enrolledRuntime(id, "container")
	producer, err := NewMutationProducer(id, runtime)
	if err != nil {
		return BindReceipt{}, err
	}
	if err := producer.Activate(id); err != nil {
		return BindReceipt{}, err
	}
	a.id, a.producer = id, producer
	return BindReceipt{Identity: id, Producer: &ProducerBinding{ContractVersion: MutationIntentVersion, Runtime: runtime,
		ProducerUID: "relay-producer", StoreScopeHash: journalTestDigest, ExecutionProfile: MutationExecutionProfile, StageIncarnation: journalTestDigest}}, nil
}
func (*relayConditionAuthority) Stop(context.Context, Identity) (StopRequest, error) {
	return StopRequest{}, ErrPending
}
func (*relayConditionAuthority) Drain(context.Context, DrainReceipt) error         { return nil }
func (*relayConditionAuthority) Exit(context.Context, StopRequest) error           { return nil }
func (*relayConditionAuthority) NeverBoundExit(context.Context, StopRequest) error { return nil }
func (*relayConditionAuthority) ReserveUpload(context.Context, string, MutationIntent) error {
	return nil
}
func (*relayConditionAuthority) SealUpload(_ context.Context, ticket string, payload []byte) (sourceSealedPayload, error) {
	digest := sha256.Sum256(payload)
	return sourceSealedPayload{Ref: "payload-" + ticket, Digest: hex.EncodeToString(digest[:]), Size: int64(len(payload))}, nil
}
func (a *relayConditionAuthority) Admit(_ context.Context, sequence uint64, intent MutationIntent) error {
	return a.producer.Admit(sequence, intent)
}
func (a *relayConditionAuthority) LookupMutation(_ context.Context, id Identity, producerUID string, sequence uint64, intent MutationIntent) (MutationLookupResult, error) {
	if id != a.id || producerUID != "relay-producer" {
		return MutationLookupResult{}, ErrClosed
	}
	entry, ok := a.producer.Entries[sequence]
	if !ok || entry.Intent != intent {
		return MutationLookupResult{}, nil
	}
	return MutationLookupResult{Found: true, Entry: &entry}, nil
}
func (a *relayConditionAuthority) Execute(_ context.Context, sequence uint64, _ MutationIntent) error {
	a.operation++
	entry := a.producer.Entries[sequence]
	preparation := MutationPreparation{Profile: MutationExecutionProfile, StoreScopeHash: entry.Intent.StoreScopeHash, GenerationSet: true}
	if err := a.producer.Prepare(sequence, preparation); err != nil {
		return err
	}
	dispatchID := fmt.Sprintf("dispatch-%d", sequence)
	if err := a.producer.GrantDispatch(sequence, dispatchID); err != nil {
		return err
	}
	if err := a.producer.SealDispatch(sequence); err != nil {
		return err
	}
	if err := a.producer.RecordDispatchTerminal(sequence, dispatchID, journalTestDigest); err != nil {
		return err
	}
	hash, err := a.producer.PreparationHash(sequence)
	if err != nil {
		return err
	}
	outcome := MutationResolution{Outcome: MutationCommitted, PreparationHash: hash, ReceiptDigest: journalTestDigest, PayloadDigest: entry.Intent.PayloadDigest, AllCopiesTerminal: true}
	if a.operation == 1 {
		outcome.Outcome, outcome.PayloadDigest, outcome.ConditionFailed = MutationRejected, "", true
	}
	if err := a.producer.Resolve(sequence, outcome); err != nil {
		return err
	}
	if outcome.ConditionFailed {
		return ErrMutationConditionFailed
	}
	return nil
}

func (*relayConditionAuthority) ArchiveGenesisStatus(context.Context, string, uint) (ArchiveGenesisStatus, error) {
	return ArchiveGenesisStatus{}, fmt.Errorf("fencing: archive genesis status is not configured in this fixture")
}

var _ sourceRelayAuthority = (*relayConditionAuthority)(nil)

func TestSourceMutationRuntimeReplansAcrossSupervisorRelayAfterConditionalRejection(t *testing.T) {
	authority := &relayConditionAuthority{}
	id := testIdentity()
	id.PodName = id.NodeID
	t.Setenv("RHIZA_POD_NAME", id.PodName)
	supervisor := &Supervisor{Authority: authority, Identity: id, requested: id, bound: id,
		grant: StartupGrant{Identity: id, Role: StartupRoleVoterActive}, ticket: "private-ticket"}
	socket := filepath.Join(t.TempDir(), "source-relay.sock")
	listener, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	server := &http.Server{Handler: supervisor.handler()}
	defer server.Close()
	defer listener.Close()
	go func() { _ = server.Serve(listener) }()
	session, err := Connect(t.Context(), socket, "private-ticket", id.SourceClusterID, id.NodeID)
	if err != nil {
		t.Fatal(err)
	}
	if err := session.BindWAL(t.Context(), "wal"); err != nil {
		t.Fatal(err)
	}
	client, err := OpenSourceMutationClient(filepath.Join(t.TempDir(), "retry"), session)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	condition := MutationCondition{Mode: MutationCreateOnly}
	if err := client.Upload(t.Context(), "target", condition, []byte("stale")); !errors.Is(err, ErrMutationConditionFailed) {
		t.Fatalf("conditional rejection was lost across relay/runtime: %v", err)
	}
	if err := client.Upload(t.Context(), "target", condition, []byte("replanned")); err != nil {
		t.Fatalf("fresh invocation remained pending after terminal rejection: %v", err)
	}
	first, second := authority.producer.Entries[1], authority.producer.Entries[2]
	if authority.operation != 2 || authority.producer.LastAdmitted != 2 || first.Resolution == nil || second.Resolution == nil || first.Resolution.Outcome != MutationRejected || second.Resolution.Outcome != MutationCommitted {
		t.Fatalf("relay did not record the rejected original and successful replan: operations=%d producer=%+v", authority.operation, authority.producer)
	}
}

func TestSourceMutationRuntimeSettlesSavedTupleBeforeNewBucketInvocation(t *testing.T) {
	inner, fake, _, _ := newSourceMutationClientTest(t)
	runtime := &SourceMutationClient{state: inner.state, client: inner}
	defer runtime.Close()
	fake.loseAdmit = true
	condition := MutationCondition{Mode: MutationCreateOnly}
	if err := runtime.Upload(context.Background(), "target", condition, []byte("first invocation")); err == nil {
		t.Fatal("lost admission response was treated as a successful Bucket write")
	}
	if len(runtime.state.data.Steps) != 1 || runtime.state.data.Steps[0].Advanced {
		t.Fatal("uncertain tuple was not retained for recovery")
	}
	fake.loseAdmit = false
	fake.result = &MutationResolution{Outcome: MutationCommitted, ReceiptDigest: journalTestDigest, AllCopiesTerminal: true}
	if err := runtime.Upload(context.Background(), "target", condition, []byte("second invocation")); err != nil {
		t.Fatalf("next invocation did not first recover its predecessor: %v", err)
	}
	if len(fake.producer.Entries) != 2 || fake.admits != 2 {
		t.Fatalf("saved and new invocations were not both admitted exactly once: entries=%d admits=%d", len(fake.producer.Entries), fake.admits)
	}
	first, second := fake.producer.Entries[1], fake.producer.Entries[2]
	if first.Intent.Key != "target" || second.Intent.Key != "target" || first.Intent.OperationID == second.Intent.OperationID || first.Intent.PayloadDigest == second.Intent.PayloadDigest {
		t.Fatal("invocation recovery deduplicated distinct calls by key or payload")
	}
	if first.Resolution == nil || second.Resolution == nil || first.Resolution.Outcome != MutationCommitted || second.Resolution.Outcome != MutationCommitted {
		t.Fatal("a Bucket invocation returned without durable source resolution")
	}
}

func TestSourceMutationRuntimeDrainRejectsUnresolvedAdmission(t *testing.T) {
	inner, fake, _, _ := newSourceMutationClientTest(t)
	runtime := &SourceMutationClient{state: inner.state, client: inner}
	fake.loseAdmit = true
	if err := runtime.Upload(context.Background(), "target", MutationCondition{Mode: MutationCreateOnly}, []byte("uncertain")); err == nil {
		t.Fatal("lost admission response was treated as a successful mutation")
	}
	if err := runtime.DrainMutations(context.Background()); !errors.Is(err, ErrPending) {
		t.Fatalf("unresolved admitted mutation received a clean drain: %v", err)
	}
	if err := runtime.Upload(context.Background(), "target", MutationCondition{Mode: MutationCreateOnly}, []byte("later")); err == nil || fake.admits != 1 {
		t.Fatalf("drained mutation client accepted further work: err=%v admits=%d", err, fake.admits)
	}
}

func TestSourceMutationRuntimeAllowsReplanAfterConditionalRejection(t *testing.T) {
	inner, fake, _, _ := newSourceMutationClientTest(t)
	runtime := &SourceMutationClient{state: inner.state, client: inner}
	defer runtime.Close()
	fake.result = &MutationResolution{Outcome: MutationRejected, ReceiptDigest: journalTestDigest, AllCopiesTerminal: true, ConditionFailed: true}
	if err := runtime.Upload(context.Background(), "target", MutationCondition{Mode: MutationCreateOnly}, []byte("rejected")); !errors.Is(err, ErrMutationConditionFailed) {
		t.Fatalf("conditional rejection did not reach the original caller: %v", err)
	}
	first := fake.producer.Entries[1]
	if first.Resolution == nil || first.Resolution.Outcome != MutationRejected || !first.Resolution.ConditionFailed {
		t.Fatal("conditional rejection was not durably resolved")
	}
	fake.result = &MutationResolution{Outcome: MutationCommitted, ReceiptDigest: journalTestDigest, AllCopiesTerminal: true}
	if err := runtime.Upload(context.Background(), "target", MutationCondition{Mode: MutationCreateOnly}, []byte("replanned")); err != nil {
		t.Fatalf("terminal condition rejection blocked a new invocation: %v", err)
	}
	if len(fake.producer.Entries) != 2 || fake.admits != 2 || fake.producer.Entries[2].Resolution == nil || fake.producer.Entries[2].Resolution.Outcome != MutationCommitted {
		t.Fatal("replan did not use a fresh successful source tuple")
	}
}

func TestSourceRetryDirectoryIsRunScoped(t *testing.T) {
	root := t.TempDir()
	first, err := SourceRetryDirectory(root, Identity{RunID: "run-a"})
	if err != nil {
		t.Fatal(err)
	}
	second, err := SourceRetryDirectory(root, Identity{RunID: "run-b"})
	if err != nil {
		t.Fatal(err)
	}
	if first == second {
		t.Fatal("separate supervisor runs share source retry identity")
	}
}

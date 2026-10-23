package fencing

import (
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
)

func enrolledRuntime(id Identity, container string) RuntimeBinding {
	return RuntimeBinding{
		AuthorityID: "authority", NamespaceUID: "namespace", BindingUID: "root",
		StatefulSetUID: "statefulset", SourceClusterID: id.SourceClusterID,
		NodeID: id.NodeID, PodUID: id.PodUID, RunID: id.RunID,
		NodeName: "node", NodeUID: "node-uid", MachineID: "machine", BootID: "boot",
		RuntimeID: "containerd", SandboxID: "sandbox", ContainerID: container, ImageID: "image",
		CapturedScopeDigest: strings.Repeat("a", 64),
	}
}

func TestRuntimeEnrollmentPrecedesRequiredWALBinding(t *testing.T) {
	id := testIdentity()
	id.WALIdentity = ""
	l := Ledger{SourceClusterID: id.SourceClusterID, RequireRuntimeBinding: true}
	if err := l.Register(id); err != nil {
		t.Fatal(err)
	}
	bound := id
	bound.WALIdentity = "wal"
	if err := l.Bind(bound); err == nil {
		t.Fatal("writer authorized before runtime enrollment")
	}
	if l.Runs[id.RunID].Identity.WALIdentity != "" {
		t.Fatal("rejected binding changed the durable WAL identity")
	}
	runtime := enrolledRuntime(id, "first-container")
	if err := l.RegisterEnrolled(id, runtime); err != nil {
		t.Fatal(err)
	}
	if err := l.RegisterEnrolled(id, runtime); err != nil {
		t.Fatalf("lost registration response is not recoverable: %v", err)
	}
	if err := l.Bind(bound); err != nil {
		t.Fatal(err)
	}
}

func TestPendingLearnerCanBindWALButCannotAcquireMutationAuthority(t *testing.T) {
	id := testIdentity()
	id.PodName = id.NodeID
	id.WALIdentity = ""
	runtime := enrolledRuntime(id, "container")
	runtime.PodName = id.PodName
	l := Ledger{SourceClusterID: id.SourceClusterID, RequireRuntimeBinding: true, RequireMutationJournal: true}
	if err := l.RegisterEnrolledWithRole(id, runtime, StartupRoleLearnerPendingRemove); err != nil {
		t.Fatal(err)
	}
	bound := id
	bound.WALIdentity = "wal"
	if err := l.Bind(bound); err != nil {
		t.Fatalf("pending learner WAL bind: %v", err)
	}
	if err := l.BindMutationProducer(bound, runtime, "producer"); err == nil {
		t.Fatal("pending learner acquired producer")
	}
	if err := l.BindMutationScope(bound, strings.Repeat("a", 64), MutationExecutionProfile); err == nil {
		t.Fatal("pending learner acquired mutation scope")
	}
	if err := l.BindMutationStage(bound, strings.Repeat("c", 64)); err == nil {
		t.Fatal("pending learner acquired mutation stage")
	}
	run := l.Runs[id.RunID]
	if run.MutationProducerUID != "" || run.MutationScopeHash != "" || run.Identity.WALIdentity != "wal" || runRole(run) != StartupRoleLearnerPendingRemove {
		t.Fatalf("learner binding changed unexpected ledger state: %+v", run)
	}
}

func TestLearnerPromotionRequiresCanonicalEvidenceAndBoundIdentity(t *testing.T) {
	id := testIdentity()
	id.NodeID = "logical-incarnation-2"
	id.PodName = "physical-ordinal-2"
	id.AssignmentID = strings.Repeat("a", 64)
	runtime := enrolledRuntime(id, "container")
	runtime.PodName = id.PodName
	runtime.AssignmentID = id.AssignmentID
	l := Ledger{SourceClusterID: id.SourceClusterID, RequireRuntimeBinding: true, RequireMutationJournal: true}
	if err := l.RegisterEnrolledWithRole(id, runtime, StartupRoleLearnerPendingRemove); err != nil {
		t.Fatal(err)
	}
	bound := id
	bound.WALIdentity = "wal"
	if err := l.Bind(bound); err != nil {
		t.Fatal(err)
	}
	evidence := strings.Repeat("b", 64)
	if err := l.PromoteLearner(bound, "remove-add-op", "not-a-digest"); err == nil {
		t.Fatal("malformed promotion evidence accepted")
	}
	if err := l.PromoteLearner(bound, "remove-add-op", evidence); err != nil {
		t.Fatal(err)
	}
	if err := l.PromoteLearner(bound, "remove-add-op", evidence); err != nil {
		t.Fatalf("exact promotion retry was not idempotent: %v", err)
	}
	if err := l.BindMutationProducer(bound, runtime, "producer"); err != nil {
		t.Fatalf("verified promotion could not bind its reserved producer: %v", err)
	}
	if err := l.BindMutationScope(bound, strings.Repeat("a", 64), MutationExecutionProfile); err != nil {
		t.Fatalf("promoted learner could not bind exact source scope: %v", err)
	}
	if err := l.BindMutationStage(bound, strings.Repeat("c", 64)); err != nil {
		t.Fatalf("promoted learner could not bind exact source stage: %v", err)
	}
	wrong := bound
	wrong.WALIdentity = "other-wal"
	if err := l.BindMutationScope(wrong, strings.Repeat("a", 64), MutationExecutionProfile); err == nil {
		t.Fatal("promoted scope accepted a different WAL identity")
	}
	run := l.Runs[id.RunID]
	if runRole(run) != StartupRoleVoterActive || run.Promotion == nil || run.Promotion.EvidenceHash != evidence || run.MutationProducerUID != "producer" || run.MutationScopeHash != strings.Repeat("a", 64) || run.MutationStageIncarnation != strings.Repeat("c", 64) {
		t.Fatalf("promotion did not persist the exact authority transition: %+v", run)
	}
}

func TestRuntimeEnrollmentRequiresEveryIdentityFieldBeforeMutation(t *testing.T) {
	id := testIdentity()
	id.NodeID = "logical-incarnation-3"
	id.PodName = "physical-ordinal-3"
	id.AssignmentID = strings.Repeat("b", 64)
	id.WALIdentity = ""
	complete := enrolledRuntime(id, "container")
	complete.PodName = id.PodName
	complete.AssignmentID = id.AssignmentID
	for field := 0; field < reflect.TypeOf(complete).NumField(); field++ {
		t.Run(reflect.TypeOf(complete).Field(field).Name, func(t *testing.T) {
			missing := complete
			reflect.ValueOf(&missing).Elem().Field(field).SetString("")
			l := Ledger{SourceClusterID: id.SourceClusterID, RequireRuntimeBinding: true}
			if reflect.TypeOf(complete).Field(field).Name == "CapturedScopeDigest" {
				if err := l.RegisterEnrolled(id, missing); err != nil || len(l.Runs) != 1 {
					t.Fatalf("legacy binding without optional captured scope digest was rejected: error=%v runs=%d", err, len(l.Runs))
				}
				return
			}
			if err := l.RegisterEnrolled(id, missing); err == nil || len(l.Runs) != 0 {
				t.Fatalf("incomplete runtime was registered: error=%v runs=%d", err, len(l.Runs))
			}
		})
	}
}

func TestCapturedScopeDigestIsValidatedAndExcludedOnlyFromScopeBindingDigest(t *testing.T) {
	id := testIdentity()
	runtime := enrolledRuntime(id, "container")
	changed := runtime
	changed.CapturedScopeDigest = strings.Repeat("b", 64)
	if RuntimeScopeBindingDigest(runtime) != RuntimeScopeBindingDigest(changed) {
		t.Fatal("scope-binding digest depends on the self-referential captured scope digest")
	}
	if RuntimeBindingDigest(runtime) == RuntimeBindingDigest(changed) {
		t.Fatal("full runtime binding digest omitted the captured scope digest")
	}
	changed.CapturedScopeDigest = "not-a-digest"
	if err := changed.Validate(id); err == nil {
		t.Fatal("malformed captured scope digest was accepted")
	}
	legacy := runtime
	legacy.CapturedScopeDigest = ""
	if err := legacy.Validate(id); err != nil {
		t.Fatalf("legacy runtime binding without a digest was rejected: %v", err)
	}
}

func TestRuntimeEnrollmentRetainsDistinctContainerAttemptsAfterReload(t *testing.T) {
	id := testIdentity()
	id.WALIdentity = ""
	l := Ledger{SourceClusterID: id.SourceClusterID, RequireRuntimeBinding: true}
	for _, container := range []string{"first", "second"} {
		id.RunID = container
		if err := l.RegisterEnrolled(id, enrolledRuntime(id, container)); err != nil {
			t.Fatal(err)
		}
		id.WALIdentity = "wal"
		if err := l.Bind(id); err != nil {
			t.Fatal(err)
		}
		id.WALIdentity = ""
	}
	data, err := json.Marshal(l)
	if err != nil {
		t.Fatal(err)
	}
	var restored Ledger
	if err := json.Unmarshal(data, &restored); err != nil {
		t.Fatal(err)
	}
	if !restored.RequireRuntimeBinding || len(restored.Runs) != 2 {
		t.Fatal("runtime admission policy/history was lost after reload")
	}
	for _, container := range []string{"first", "second"} {
		run := restored.Runs[container]
		if run.Runtime == nil || run.Runtime.ContainerID != container || run.Identity.WALIdentity != "wal" {
			t.Fatalf("container attempt was replaced by the latest one: %+v", run)
		}
	}
}

func TestRuntimeEnrollmentRejectsRebindingAndClosureWithoutChangingHistory(t *testing.T) {
	id := testIdentity()
	id.WALIdentity = ""
	l := Ledger{SourceClusterID: id.SourceClusterID, RequireRuntimeBinding: true}
	runtime := enrolledRuntime(id, "container")
	if err := l.RegisterEnrolled(id, runtime); err != nil {
		t.Fatal(err)
	}
	for _, mutate := range []func(*RuntimeBinding){
		func(b *RuntimeBinding) { b.ContainerID = "replacement" },
		func(b *RuntimeBinding) { b.NodeUID = "replacement" },
		func(b *RuntimeBinding) { b.BootID = "replacement" },
	} {
		changed := runtime
		mutate(&changed)
		if err := l.RegisterEnrolled(id, changed); err == nil {
			t.Fatal("same run accepted a different runtime incarnation")
		}
		if *l.Runs[id.RunID].Runtime != runtime {
			t.Fatal("rejected enrollment changed history")
		}
	}
	if err := l.Close("close-operation", "request-hash"); err != nil {
		t.Fatal(err)
	}
	next := id
	next.RunID = "late-run"
	if err := l.RegisterEnrolled(next, enrolledRuntime(next, "late-container")); !errors.Is(err, ErrClosed) {
		t.Fatalf("enrollment crossed permanent closure: %v", err)
	}
	if len(l.Runs) != 1 {
		t.Fatal("closed ledger gained another runtime")
	}
}

func TestRuntimeEnrollmentPinsProducerUIDBeforeWALAdmission(t *testing.T) {
	id := testIdentity()
	id.WALIdentity = ""
	l := Ledger{SourceClusterID: id.SourceClusterID, RequireMutationJournal: true}
	runtime := enrolledRuntime(id, "container")
	if err := l.RegisterEnrolled(id, runtime); err != nil {
		t.Fatal(err)
	}
	bound := id
	bound.WALIdentity = "wal"
	if err := l.Bind(bound); err == nil {
		t.Fatal("writer bound before producer UID enrollment")
	}
	if err := l.BindMutationProducer(id, runtime, "producer-uid"); err != nil {
		t.Fatal(err)
	}
	if err := l.Bind(bound); err != nil {
		t.Fatal(err)
	}
	if err := l.BindMutationScope(bound, strings.Repeat("a", 64), MutationExecutionProfile); err == nil {
		t.Fatal("ordinary active voter retrofitted mutation scope after WAL activation")
	}
	if l.Runs[id.RunID].MutationScopeHash != "" {
		t.Fatal("rejected late scope changed durable voter state")
	}
	if err := l.BindMutationProducer(id, runtime, "producer-uid"); err != nil {
		t.Fatal("lost enrollment response not resolvable:", err)
	}
	if err := l.BindMutationProducer(id, runtime, "replacement-uid"); err == nil {
		t.Fatal("producer replacement changed immutable enrollment")
	}
	data, err := json.Marshal(l)
	if err != nil {
		t.Fatal(err)
	}
	var restored Ledger
	if err := json.Unmarshal(data, &restored); err != nil {
		t.Fatal(err)
	}
	if !restored.RequireMutationJournal || restored.Runs[id.RunID].MutationProducerUID != "producer-uid" {
		t.Fatal("producer gate or identity lost after authority restart")
	}
	if err := restored.Bind(bound); err != nil {
		t.Fatal(err)
	}
}

func TestRuntimeEnrollmentCannotRetrofitJournalAfterWALBinding(t *testing.T) {
	id := testIdentity()
	id.WALIdentity = ""
	l := Ledger{SourceClusterID: id.SourceClusterID, RequireRuntimeBinding: true}
	runtime := enrolledRuntime(id, "container")
	if err := l.RegisterEnrolled(id, runtime); err != nil {
		t.Fatal(err)
	}
	bound := id
	bound.WALIdentity = "legacy-wal"
	if err := l.Bind(bound); err != nil {
		t.Fatal(err)
	}
	if err := l.BindMutationProducer(id, runtime, "late-uid"); err == nil {
		t.Fatal("already authorized writer acquired fictitious empty journal history")
	}
	if l.Runs[id.RunID].MutationProducerUID != "" {
		t.Fatal("rejected retrofit mutated history")
	}
}

package operator

import (
	"strings"
	"testing"

	"github.com/mrchypark/rhiza/pkg/fencing"
)

func TestProducerBindingRecoversLostRootOrActivationResponse(t *testing.T) {
	for _, lost := range []string{"root", "producer"} {
		t.Run(lost, func(t *testing.T) {
			e, f, id, runtime := newMutationStoreFixture(t)
			if _, err := e.reserveMutationProducer(t.Context(), runtime); err != nil {
				t.Fatal(err)
			}
			run, err := e.enrolledMutationRun(t.Context(), runtime)
			if err != nil {
				t.Fatal(err)
			}
			f.mu.Lock()
			f.lostLedgerResponse, f.lostResponse = lost == "root", lost == "producer"
			f.mu.Unlock()
			id.WALIdentity = "wal"
			if receipt, err := e.bindWALAtScope(t.Context(), "sts", id, strings.Repeat("a", 64)); err == nil || receipt.Producer != nil {
				t.Fatal("lost response became a usable binding")
			}
			restarted := &FencingExecutor{Kube: e.Kube, AuthorityState: e.AuthorityState}
			receipt, err := restarted.bindWALAtScope(t.Context(), "sts", id, strings.Repeat("a", 64))
			if err != nil || receipt.Validate(id) != nil || receipt.Producer == nil || receipt.Producer.ProducerUID != run.MutationProducerUID {
				t.Fatalf("lost bind did not recover same producer: %+v %v", receipt, err)
			}
			p := readFixtureProducer(t, e, f, runtime)
			if p.Phase != fencing.MutationOpen || p.WALIdentity != id.WALIdentity || p.LastAdmitted != 0 {
				t.Fatal("binding did not durably activate exact WAL or performed a mutation")
			}
		})
	}
}

func TestProducerBindingPinsImmutableSourceStageIncarnation(t *testing.T) {
	e, _, id, runtime := newMutationStoreFixture(t)
	if _, err := e.reserveMutationProducer(t.Context(), runtime); err != nil {
		t.Fatal(err)
	}
	id.WALIdentity = "wal"
	incarnation := strings.Repeat("4", 64)
	receipt, err := e.bindWALAtScopeWithStage(t.Context(), "sts", id, strings.Repeat("a", 64), incarnation)
	if err != nil || receipt.Validate(id) != nil || receipt.Producer == nil || receipt.Producer.StageIncarnation != incarnation {
		t.Fatalf("stage incarnation was not returned in the binding: %+v %v", receipt, err)
	}
	state, err := e.read(t.Context(), "sts", "source")
	if err != nil || state.Ledger.Runs[id.RunID].MutationStageIncarnation != incarnation {
		t.Fatalf("stage incarnation was not pinned in the root ledger: %+v %v", state.Ledger.Runs[id.RunID], err)
	}
	restarted := &FencingExecutor{Kube: e.Kube, AuthorityState: e.AuthorityState}
	if retry, err := restarted.bindWALAtScopeWithStage(t.Context(), "sts", id, strings.Repeat("a", 64), incarnation); err != nil || !receipt.SameBinding(retry) {
		t.Fatalf("same incarnation did not recover the original binding: %+v %v", retry, err)
	}
	if retry, err := restarted.bindWALAtScopeWithStage(t.Context(), "sts", id, strings.Repeat("a", 64), strings.Repeat("5", 64)); err == nil || retry.Producer != nil {
		t.Fatal("replacement source stage incarnation returned a producer binding")
	}
	if retry, err := restarted.bindWALAtScope(t.Context(), "sts", id, strings.Repeat("a", 64)); err == nil || retry.Producer != nil {
		t.Fatal("missing original source stage was treated as a fresh binding")
	}
}

func TestProducerBindingRejectsClosureScopeAndReplacedStorage(t *testing.T) {
	for _, fault := range []string{"closure", "scope", "missing", "replaced"} {
		t.Run(fault, func(t *testing.T) {
			e, f, id, runtime := newMutationStoreFixture(t)
			if _, err := e.reserveMutationProducer(t.Context(), runtime); err != nil {
				t.Fatal(err)
			}
			id.WALIdentity = "wal"
			scope := strings.Repeat("a", 64)
			if _, err := e.bindWALAtScope(t.Context(), "sts", id, scope); err != nil {
				t.Fatal(err)
			}
			switch fault {
			case "closure":
				if _, err := e.change(t.Context(), "sts", "source", func(l *fencing.Ledger) error { return l.Close("fence", strings.Repeat("b", 64)) }); err != nil {
					t.Fatal(err)
				}
			case "scope":
				scope = strings.Repeat("b", 64)
			case "missing":
				f.mu.Lock()
				delete(f.objects, e.mutationProducerPath(runtime))
				f.mu.Unlock()
			case "replaced":
				f.mu.Lock()
				asObject(f.objects[e.mutationProducerPath(runtime)]["metadata"])["uid"] = "replacement"
				f.mu.Unlock()
			}
			if receipt, err := e.bindWALAtScope(t.Context(), "sts", id, scope); err == nil || receipt.Producer != nil {
				t.Fatal("unverifiable producer returned a binding")
			}
		})
	}
}

func TestProducerBindingChecksActualPodAndTemplateScope(t *testing.T) {
	for _, changed := range []bool{false, true} {
		t.Run(map[bool]string{false: "same store", true: "template changed"}[changed], func(t *testing.T) {
			e, f, id, runtime := newMutationStoreFixture(t)
			if _, err := e.reserveMutationProducer(t.Context(), runtime); err != nil {
				t.Fatal(err)
			}
			spec := func(prefix string) object {
				return object{"containers": []any{object{"env": []any{
					object{"name": "RHIZA_OBJSTORE_PROVIDER", "value": "gcs"},
					object{"name": "RHIZA_OBJSTORE_BUCKET", "value": "test-bucket"},
					object{"name": "RHIZA_OBJSTORE_PREFIX", "value": prefix},
				}}}}
			}
			prefix := "test-prefix"
			if changed {
				prefix = "another-prefix"
			}
			f.mu.Lock()
			f.objects[e.controller().corePath("pods", id.NodeID)] = object{
				"metadata": object{"uid": id.PodUID, "ownerReferences": []any{object{"kind": "StatefulSet", "uid": "sts", "name": "db"}}}, "spec": spec("test-prefix")}
			f.objects[e.controller().stsPath("db")] = object{
				"metadata": object{"uid": "sts"}, "spec": object{"template": object{"spec": spec(prefix)}}}
			f.mu.Unlock()
			id.WALIdentity = "wal"
			receipt, err := e.bindSupervisorWAL(t.Context(), "sts", id)
			if changed {
				if err == nil || receipt.Producer != nil {
					t.Fatal("changed template enabled source binding")
				}
				state, readErr := e.read(t.Context(), "sts", "source")
				if readErr != nil || state.Ledger.Runs[id.RunID].Identity.WALIdentity != "" {
					t.Fatal("scope rejection partially bound WAL")
				}
			} else if err != nil || receipt.Producer == nil || receipt.Validate(id) != nil {
				t.Fatalf("matching trusted scopes failed: %+v %v", receipt, err)
			}
		})
	}
}

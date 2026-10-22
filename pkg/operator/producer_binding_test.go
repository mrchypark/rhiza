package operator

import (
	"strings"
	"testing"

	"github.com/mrchypark/rhiza/pkg/fencing"
	"github.com/thanos-io/objstore"
)

func preparePendingArchiveGenesis(t *testing.T, e *FencingExecutor, fixture *mutationStoreFixture, id fencing.Identity, runtime fencing.RuntimeBinding) string {
	t.Helper()
	e.RequireMutationJournal = true
	e.ArchiveBucket = objstore.NewInMemBucket()
	bucket := e.ArchiveBucket
	t.Cleanup(func() { _ = bucket.Close() })
	if _, err := e.change(t.Context(), runtime.StatefulSetUID, runtime.SourceClusterID, func(l *fencing.Ledger) error {
		l.ArchiveGenesisState = fencing.ArchiveGenesisRegistrationPending
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	env := []any{
		object{"name": "RHIZA_CLUSTER_ID", "value": runtime.SourceClusterID},
		object{"name": "RHIZA_OBJSTORE_PROVIDER", "value": "gcs"},
		object{"name": "RHIZA_OBJSTORE_BUCKET", "value": "archive"},
		object{"name": "RHIZA_OBJSTORE_PREFIX", "value": "root"},
	}
	pod := supervisedTestPod()
	asObject(pod["metadata"])["name"] = id.NodeID
	asObject(pod["metadata"])["uid"] = id.PodUID
	asObject(pod["metadata"])["ownerReferences"] = []any{object{"kind": "StatefulSet", "name": "rhiza", "uid": runtime.StatefulSetUID, "controller": true}}
	asObject(list(nested(pod, "spec", "containers"))[0])["env"] = env
	sts := object{"metadata": object{"uid": runtime.StatefulSetUID}, "spec": object{"template": object{"spec": object{"containers": []any{object{"name": "rhiza", "env": env}}}}}}
	fixture.mu.Lock()
	fixture.authPod, fixture.authStatefulSet = pod, sts
	fixture.objects["/apis/apps/v1/namespaces/test/statefulsets"] = object{"items": []any{sts}}
	fixture.mu.Unlock()
	if err := e.claimArchiveScopeForRegistration(t.Context(), runtime.StatefulSetUID, id, runtime); err != nil {
		t.Fatal(err)
	}
	fixture.mu.Lock()
	fixture.authPod, fixture.authStatefulSet = nil, nil
	fixture.mu.Unlock()
	_, scopeHash, err := e.archiveGenesisScope(t.Context(), runtime)
	if err != nil {
		t.Fatal(err)
	}
	return scopeHash
}

func TestProducerBindingRecoversLostRootOrActivationResponse(t *testing.T) {
	for _, lost := range []string{"root", "producer", "root-unknown"} {
		t.Run(lost, func(t *testing.T) {
			e, f, id, runtime := newMutationStoreFixture(t)
			scope := preparePendingArchiveGenesis(t, e, f, id, runtime)
			if _, err := e.reserveMutationProducer(t.Context(), runtime); err != nil {
				t.Fatal(err)
			}
			run, err := e.enrolledMutationRun(t.Context(), runtime)
			if err != nil {
				t.Fatal(err)
			}
			faultBucket := &mutationCASReplyFaultBucket{
				Bucket:             f.authorityBucket,
				loseCASAfter:       1,
				failReadAfterReply: lost == "root-unknown",
			}
			if lost == "producer" {
				faultBucket.loseCASAfter = 2 // root bind, then producer activation
			}
			e.AuthorityState.bucket = faultBucket
			id.WALIdentity = "wal"
			receipt, firstErr := e.bindWALAtScope(t.Context(), "sts", id, scope)
			if lost == "root-unknown" {
				if firstErr == nil || receipt.Producer != nil {
					t.Fatal("CAS response plus failed readback was not kept uncertain")
				}
			} else if firstErr != nil || receipt.Validate(id) != nil || receipt.Producer == nil {
				t.Fatalf("exact canonical readback did not resolve the lost response: %+v %v", receipt, firstErr)
			}
			restarted := &FencingExecutor{Kube: e.Kube, AuthorityState: e.AuthorityState}
			receipt, err = restarted.bindWALAtScope(t.Context(), "sts", id, scope)
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

func TestMutationJournalBindingRejectsCooperativeRootAndPreservesCooperativeBinding(t *testing.T) {
	t.Run("required executor rejects stale cooperative root without mutation", func(t *testing.T) {
		store, _ := newAuthorityStateStoreForTest(t)
		cooperative := &FencingExecutor{AuthorityState: store}
		id := fencing.Identity{SourceClusterID: "source", NodeID: "node", PodUID: "pod", RunID: "run"}
		if err := cooperative.initializeRoot(t.Context(), "sts", "source"); err != nil {
			t.Fatal(err)
		}
		if _, err := cooperative.change(t.Context(), "sts", "source", func(l *fencing.Ledger) error { return l.Register(id) }); err != nil {
			t.Fatal(err)
		}
		id.WALIdentity = "wal"
		_, beforeVersion, err := store.ReadRoot(t.Context(), "sts", "source")
		if err != nil {
			t.Fatal(err)
		}
		required := &FencingExecutor{AuthorityState: store, RequireMutationJournal: true}
		if receipt, err := required.bindWALAtScope(t.Context(), "sts", id, strings.Repeat("a", 64)); err == nil || receipt.Producer != nil {
			t.Fatalf("stale cooperative root returned required binding: %+v %v", receipt, err)
		}
		_, afterVersion, err := store.ReadRoot(t.Context(), "sts", "source")
		if err != nil || beforeVersion != afterVersion {
			t.Fatalf("profile mismatch mutated root version: %v", err)
		}
	})

	t.Run("explicit cooperative root may bind without producer", func(t *testing.T) {
		store, _ := newAuthorityStateStoreForTest(t)
		executor := &FencingExecutor{AuthorityState: store}
		id := fencing.Identity{SourceClusterID: "source", NodeID: "node", PodUID: "pod", RunID: "run"}
		if err := executor.initializeRoot(t.Context(), "sts", "source"); err != nil {
			t.Fatal(err)
		}
		if _, err := executor.change(t.Context(), "sts", "source", func(l *fencing.Ledger) error { return l.Register(id) }); err != nil {
			t.Fatal(err)
		}
		id.WALIdentity = "wal"
		receipt, err := executor.bindWALAtScope(t.Context(), "sts", id, "")
		if err != nil || receipt.Producer != nil || receipt.Validate(id) != nil {
			t.Fatalf("cooperative bind did not retain nil-producer contract: %+v %v", receipt, err)
		}
	})
}

func TestProducerBindingPinsImmutableSourceStageIncarnation(t *testing.T) {
	e, fixture, id, runtime := newMutationStoreFixture(t)
	scope := preparePendingArchiveGenesis(t, e, fixture, id, runtime)
	if _, err := e.reserveMutationProducer(t.Context(), runtime); err != nil {
		t.Fatal(err)
	}
	id.WALIdentity = "wal"
	incarnation := strings.Repeat("4", 64)
	receipt, err := e.bindWALAtScopeWithStage(t.Context(), "sts", id, scope, incarnation)
	if err != nil || receipt.Validate(id) != nil || receipt.Producer == nil || receipt.Producer.StageIncarnation != incarnation {
		t.Fatalf("stage incarnation was not returned in the binding: %+v %v", receipt, err)
	}
	state, err := e.read(t.Context(), "sts", "source")
	if err != nil || state.Ledger.Runs[id.RunID].MutationStageIncarnation != incarnation {
		t.Fatalf("stage incarnation was not pinned in the root ledger: %+v %v", state.Ledger.Runs[id.RunID], err)
	}
	restarted := &FencingExecutor{Kube: e.Kube, AuthorityState: e.AuthorityState}
	if retry, err := restarted.bindWALAtScopeWithStage(t.Context(), "sts", id, scope, incarnation); err != nil || !receipt.SameBinding(retry) {
		t.Fatalf("same incarnation did not recover the original binding: %+v %v", retry, err)
	}
	if retry, err := restarted.bindWALAtScopeWithStage(t.Context(), "sts", id, scope, strings.Repeat("5", 64)); err == nil || retry.Producer != nil {
		t.Fatal("replacement source stage incarnation returned a producer binding")
	}
	if retry, err := restarted.bindWALAtScope(t.Context(), "sts", id, scope); err == nil || retry.Producer != nil {
		t.Fatal("missing original source stage was treated as a fresh binding")
	}
}

func TestProducerBindingRejectsClosureScopeAndReplacedStorage(t *testing.T) {
	for _, fault := range []string{"closure", "scope", "missing", "replaced"} {
		t.Run(fault, func(t *testing.T) {
			e, f, id, runtime := newMutationStoreFixture(t)
			scope := preparePendingArchiveGenesis(t, e, f, id, runtime)
			if _, err := e.reserveMutationProducer(t.Context(), runtime); err != nil {
				t.Fatal(err)
			}
			id.WALIdentity = "wal"
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
				key, _, err := e.AuthorityState.producerKey(runtime)
				if err != nil {
					t.Fatal(err)
				}
				if err := f.authorityBucket.Delete(t.Context(), key); err != nil {
					t.Fatal(err)
				}
			case "replaced":
				p, _, version, err := e.AuthorityState.ReadProducer(t.Context(), runtime)
				if err != nil {
					t.Fatal(err)
				}
				key, identity, err := e.AuthorityState.producerKey(runtime)
				if err != nil {
					t.Fatal(err)
				}
				value := struct {
					ProducerUID string                   `json:"producerUID"`
					Producer    fencing.MutationProducer `json:"producer"`
				}{"replacement-uid", p}
				if err := e.AuthorityState.write(t.Context(), key, identity, value, version); err != nil {
					t.Fatal(err)
				}
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
			_ = preparePendingArchiveGenesis(t, e, f, id, runtime)
			if _, err := e.reserveMutationProducer(t.Context(), runtime); err != nil {
				t.Fatal(err)
			}
			spec := func(prefix string) object {
				return object{"containers": []any{object{"env": []any{
					object{"name": "RHIZA_OBJSTORE_PROVIDER", "value": "gcs"},
					object{"name": "RHIZA_OBJSTORE_BUCKET", "value": "archive"},
					object{"name": "RHIZA_OBJSTORE_PREFIX", "value": prefix},
				}}}}
			}
			prefix := "root"
			if changed {
				prefix = "another-prefix"
			}
			f.mu.Lock()
			f.objects[e.controller().corePath("pods", id.NodeID)] = object{
				"metadata": object{"uid": id.PodUID, "ownerReferences": []any{object{"kind": "StatefulSet", "uid": "sts", "name": "db"}}}, "spec": spec("root")}
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

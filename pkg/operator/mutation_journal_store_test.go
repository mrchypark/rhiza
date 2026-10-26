package operator

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/mrchypark/rhiza/pkg/fencing"
	"github.com/thanos-io/objstore"
)

func completeStoreIntent(intent fencing.MutationIntent) fencing.MutationIntent {
	intent.ContractVersion = fencing.MutationIntentVersion
	if intent.StoreScopeHash == "" {
		intent.StoreScopeHash = strings.Repeat("a", 64)
	}
	intent.OwnerStep = fencing.MutationOwnerStep{Owner: "fixture", Workflow: "publication", Step: intent.OperationID, Attempt: 1}
	if intent.Kind == "delete" {
		intent.Condition = fencing.MutationCondition{Mode: fencing.MutationMatchGeneration, Generation: 12}
	} else {
		intent.Condition = fencing.MutationCondition{Mode: fencing.MutationUnconditional}
		intent.PayloadSize = 1
	}
	return intent
}

type mutationStoreFixture struct {
	mu                    sync.Mutex
	objects               map[string]object
	version               int
	conflicts             int
	lostResponse          bool
	lostArchiveResponse   bool
	lostTerminalResponse  bool
	lostLedgerResponse    bool
	archiveReads          int
	archivePosts          int
	archiveReadError      string
	statusWrites          int
	lastEmptyRecoveryHTTP string
	authorityBucket       objstore.Bucket
	// Optional Kubernetes auth fixtures for testing the bound-token HTTP route.
	authPod         object
	authPodName     string
	authPodUID      string
	authStatefulSet object
}

func (f *mutationStoreFixture) serve(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if r.URL.Path == "/apis/authentication.k8s.io/v1/tokenreviews" && r.Method == http.MethodPost {
		podName, podUID := f.authPodName, f.authPodUID
		if podName == "" {
			podName = "n0"
		}
		if podUID == "" {
			podUID = "pod-0"
		}
		_ = json.NewEncoder(w).Encode(object{"status": object{
			"authenticated": true, "audiences": []any{"rhiza-fencer"},
			"user": object{"username": "system:serviceaccount:test:default", "extra": object{
				"authentication.kubernetes.io/pod-uid":  []any{podUID},
				"authentication.kubernetes.io/pod-name": []any{podName},
			}},
		}})
		return
	}
	if r.Method == http.MethodGet && f.authPod != nil && r.URL.Path == "/api/v1/namespaces/test/pods/"+str(nested(f.authPod, "metadata", "name")) {
		_ = json.NewEncoder(w).Encode(f.authPod)
		return
	}
	if r.Method == http.MethodGet && r.URL.Path == "/apis/apps/v1/namespaces/test/statefulsets/rhiza" && f.authStatefulSet != nil {
		if current := f.objects[r.URL.Path]; current != nil {
			_ = json.NewEncoder(w).Encode(current)
		} else {
			_ = json.NewEncoder(w).Encode(f.authStatefulSet)
		}
		return
	}
	current := f.objects[r.URL.Path]
	if r.Method == http.MethodGet {
		if strings.Contains(r.URL.Path, "/rhiza-mutation-archive-") {
			f.archiveReads++
			if f.archiveReadError == r.URL.Path {
				w.WriteHeader(503)
				return
			}
		}
		if current == nil {
			w.WriteHeader(404)
			return
		}
		_ = json.NewEncoder(w).Encode(current)
		return
	}
	if r.Method != http.MethodPost && r.Method != http.MethodPut {
		w.WriteHeader(405)
		return
	}
	var next object
	if json.NewDecoder(r.Body).Decode(&next) != nil {
		w.WriteHeader(400)
		return
	}
	if r.Method == http.MethodPost {
		// Kubernetes populates metadata.namespace from a namespaced collection
		// endpoint when the submitted object omits it. Preserve that API behavior
		// before assigning UID/resourceVersion and returning the created object.
		parts := strings.Split(r.URL.Path, "/")
		for i := 0; i+1 < len(parts); i++ {
			if parts[i] == "namespaces" && parts[i+1] != "" {
				if metadata := asObject(next["metadata"]); metadata != nil && str(metadata["namespace"]) == "" {
					metadata["namespace"] = parts[i+1]
				}
				break
			}
		}
	}
	if r.Method == http.MethodPut && strings.HasSuffix(r.URL.Path, "/status") {
		f.objects[r.URL.Path] = next
		resourcePath := strings.TrimSuffix(r.URL.Path, "/status")
		if resource := f.objects[resourcePath]; resource != nil {
			// Kubernetes status-subresource writes persist status on the primary
			// object; a later GET of that object must observe the updated status
			// without replacing its spec or metadata with the submitted copy.
			resource["status"] = next["status"]
			if nextMetadata := asObject(next["metadata"]); nextMetadata != nil {
				if resourceMetadata := asObject(resource["metadata"]); resourceMetadata != nil {
					if resourceVersion := str(nextMetadata["resourceVersion"]); resourceVersion != "" {
						resourceMetadata["resourceVersion"] = resourceVersion
					}
				}
			}
			f.objects[resourcePath] = resource
			_ = json.NewEncoder(w).Encode(resource)
			f.statusWrites++
			return
		}
		f.statusWrites++
		w.WriteHeader(200)
		return
	}
	endpoint := r.URL.Path
	if r.Method == http.MethodPost {
		endpoint += "/" + str(nested(next, "metadata", "name"))
		current = f.objects[endpoint]
	}
	producer := strings.Contains(endpoint, "/rhiza-mutations-")
	archive := strings.Contains(endpoint, "/rhiza-mutation-archive-")
	if archive && r.Method == http.MethodPost {
		f.archivePosts++
	}
	if producer && f.conflicts > 0 {
		f.conflicts--
		w.WriteHeader(409)
		return
	}
	if (r.Method == http.MethodPost && current != nil) || (r.Method == http.MethodPut && (current == nil || str(nested(next, "metadata", "resourceVersion")) != str(nested(current, "metadata", "resourceVersion")) || str(nested(next, "metadata", "uid")) != str(nested(current, "metadata", "uid")))) {
		w.WriteHeader(409)
		return
	}
	if current != nil && current["immutable"] == true && (next["immutable"] != true || hashJSON(next["data"]) != hashJSON(current["data"]) || hashJSON(next["binaryData"]) != hashJSON(current["binaryData"])) {
		w.WriteHeader(422)
		return
	}
	f.version++
	metadata := asObject(next["metadata"])
	metadata["resourceVersion"] = fmt.Sprint(f.version)
	if current == nil {
		metadata["uid"] = fmt.Sprintf("uid-%d", f.version)
	} else {
		metadata["uid"] = nested(current, "metadata", "uid")
	}
	f.objects[endpoint] = next
	if strings.Contains(endpoint, "/rhiza-terminal-") && f.lostTerminalResponse {
		f.lostTerminalResponse = false
		w.WriteHeader(500)
		return
	}
	if strings.Contains(endpoint, "/rhiza-fence-ledger-") && f.lostLedgerResponse {
		f.lostLedgerResponse = false
		w.WriteHeader(500)
		return
	}
	if archive && f.lostArchiveResponse {
		f.lostArchiveResponse = false
		w.WriteHeader(500)
		return
	}
	if producer && f.lostResponse {
		f.lostResponse = false
		w.WriteHeader(500)
		return
	}
	if r.Method == http.MethodPut {
		// Kubernetes returns the persisted object for a successful update. Keep
		// this fake response faithful for callers that verify UID/resourceVersion.
		w.Header().Set("Content-Type", "application/json")
	}
	w.WriteHeader(http.StatusOK)
	if r.Method == http.MethodPut {
		_ = json.NewEncoder(w).Encode(next)
	}
}

func TestMutationStoreStatusWriteUpdatesPrimaryResource(t *testing.T) {
	const resourcePath = "/apis/rhiza.mrchypark.dev/v1alpha1/namespaces/test/rhizarecoveries/recovery-a"
	initial := object{
		"apiVersion": "rhiza.mrchypark.dev/v1alpha1",
		"kind":       "RhizaRecovery",
		"metadata": object{
			"name": "recovery-a", "namespace": "test", "uid": "recovery-uid",
			"resourceVersion": "fixture-rv", "labels": object{"fixture": "primary"},
		},
		"spec":   object{"recoveryID": "original-operation", "marker": "original-spec"},
		"status": object{"phase": "Pending", "marker": "old-status"},
	}
	fixture := &mutationStoreFixture{objects: map[string]object{resourcePath: initial}}
	api := httptest.NewTLSServer(http.HandlerFunc(fixture.serve))
	t.Cleanup(api.Close)
	tokenFile := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(tokenFile, []byte("test"), 0600); err != nil {
		t.Fatal(err)
	}
	kube := &Kubernetes{BaseURL: api.URL, Namespace: "test", TokenFile: tokenFile, Client: api.Client()}

	// Deliberately distinguish the submitted body from the primary object's
	// spec/labels: a status-subresource write changes status, not those fields.
	// resourceVersion here is just fixture data; this fake does not model API
	// server version increments or optimistic-concurrency behavior.
	update := object{
		"apiVersion": "rhiza.mrchypark.dev/v1alpha1",
		"kind":       "RhizaRecovery",
		"metadata": object{
			"name": "recovery-a", "namespace": "test", "uid": "recovery-uid",
			"resourceVersion": "fixture-rv", "labels": object{"fixture": "submitted"},
		},
		"spec":   object{"recoveryID": "wrong-operation", "marker": "submitted-spec"},
		"status": object{"phase": "Recovering", "marker": "new-status"},
	}
	var response object
	if err := kube.Put(t.Context(), resourcePath+"/status", update, &response); err != nil {
		t.Fatalf("PUT status subresource: %v", err)
	}
	var got object
	if err := kube.Get(t.Context(), resourcePath, &got); err != nil {
		t.Fatalf("GET primary resource after status update: %v", err)
	}
	wantMetadata := asObject(initial["metadata"])
	if hashJSON(got["spec"]) != hashJSON(initial["spec"]) || hashJSON(got["metadata"]) != hashJSON(wantMetadata) {
		t.Fatalf("status write replaced primary spec or metadata: got spec=%v metadata=%v", got["spec"], got["metadata"])
	}
	wantStatus := update["status"]
	if hashJSON(got["status"]) != hashJSON(wantStatus) || hashJSON(response["status"]) != hashJSON(wantStatus) {
		t.Fatalf("status write was not visible on primary resource and response: GET=%v PUT=%v", got["status"], response["status"])
	}
}

func newMutationStoreFixture(t *testing.T) (*FencingExecutor, *mutationStoreFixture, fencing.Identity, fencing.RuntimeBinding) {
	t.Helper()
	f := &mutationStoreFixture{objects: make(map[string]object)}
	api := httptest.NewTLSServer(http.HandlerFunc(f.serve))
	t.Cleanup(api.Close)
	token := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(token, []byte("test"), 0600); err != nil {
		t.Fatal(err)
	}
	stateBucket := objstore.NewInMemBucket()
	t.Cleanup(func() { _ = stateBucket.Close() })
	state, err := NewAuthorityStateStore(stateBucket, "test-authority")
	if err != nil {
		t.Fatal(err)
	}
	f.authorityBucket = stateBucket
	e := &FencingExecutor{Kube: &Kubernetes{BaseURL: api.URL, Namespace: "test", TokenFile: token, Client: api.Client()}, AuthorityState: state}
	id := fencing.Identity{SourceClusterID: "source", NodeID: "n0", PodUID: "pod-0", RunID: "run-0"}
	runtime := fencing.RuntimeBinding{
		AuthorityID: "authority", NamespaceUID: "namespace", BindingUID: "binding", StatefulSetUID: "sts",
		SourceClusterID: id.SourceClusterID, NodeID: id.NodeID, PodUID: id.PodUID, RunID: id.RunID,
		NodeName: "node", NodeUID: "node-uid", MachineID: "machine", BootID: "boot", RuntimeID: "containerd", SandboxID: "sandbox", ContainerID: "container", ImageID: "image",
	}
	if err := e.initializeRoot(t.Context(), runtime.StatefulSetUID, runtime.SourceClusterID); err != nil {
		t.Fatal(err)
	}
	_, err = e.change(t.Context(), "sts", "source", func(l *fencing.Ledger) error {
		l.RequireRuntimeBinding = true
		l.RequireMutationJournal = true
		return l.RegisterEnrolled(id, runtime)
	})
	if err != nil {
		t.Fatal(err)
	}
	return e, f, id, runtime
}

func activateStoredProducer(t *testing.T, e *FencingExecutor, id fencing.Identity, runtime fencing.RuntimeBinding) {
	t.Helper()
	if _, err := e.reserveMutationProducer(t.Context(), runtime); err != nil {
		t.Fatal(err)
	}
	id.WALIdentity = "wal"
	if _, err := e.change(t.Context(), "sts", "source", func(l *fencing.Ledger) error { return l.Bind(id) }); err != nil {
		t.Fatal(err)
	}
	if _, err := e.changeMutationProducer(t.Context(), runtime, func(p *fencing.MutationProducer) error { return p.Activate(id) }); err != nil {
		t.Fatal(err)
	}
}

var errInjectedMutationProducerCASConflict = errors.New("injected producer conditional-write conflict")

type forcedProducerCASConflictBucket struct {
	objstore.Bucket
	key       string
	remaining int
}

func (b *forcedProducerCASConflictBucket) Upload(ctx context.Context, name string, reader io.Reader, options ...objstore.ObjectUploadOption) error {
	params := objstore.ApplyObjectUploadOptions(options...)
	if name == b.key && params.Condition != nil && b.remaining > 0 {
		b.remaining--
		return errInjectedMutationProducerCASConflict
	}
	return b.Bucket.Upload(ctx, name, reader, options...)
}

func (b *forcedProducerCASConflictBucket) IsConditionNotMetErr(err error) bool {
	return errors.Is(err, errInjectedMutationProducerCASConflict) || b.Bucket.IsConditionNotMetErr(err)
}

func TestMutationJournalStoreClosureIncludesMissingReservedProducer(t *testing.T) {
	e, _, _, runtime := newMutationStoreFixture(t)
	digest := strings.Repeat("a", 64)
	state, err := e.change(t.Context(), "sts", "source", func(l *fencing.Ledger) error { return l.Close("fence", digest) })
	if err != nil {
		t.Fatal(err)
	}
	complete, err := e.closeMutationProducers(t.Context(), state, nil)
	if err != nil || !complete {
		t.Fatalf("missing never-bound producer not closed: complete=%v error=%v", complete, err)
	}
	// A fresh executor simulates authority restart; it reads the same closed
	// object and cannot initialize a new open value from absent process memory.
	restarted := &FencingExecutor{Kube: e.Kube, AuthorityState: e.AuthorityState}
	p, err := restarted.reserveMutationProducer(t.Context(), runtime)
	if err != nil || p.Phase != fencing.MutationClosed || !p.BarrierComplete() {
		t.Fatalf("closure not durable: %+v %v", p, err)
	}
	_, err = restarted.changeMutationProducer(t.Context(), runtime, func(p *fencing.MutationProducer) error {
		return p.Admit(1, completeStoreIntent(fencing.MutationIntent{OperationID: "late", Kind: "delete", Key: "key"}))
	})
	if !errors.Is(err, fencing.ErrClosed) {
		t.Fatalf("late admission crossed closure: %v", err)
	}
}

func TestMutationJournalStoreMissingBoundStateCannotBeRecreated(t *testing.T) {
	e, f, id, runtime := newMutationStoreFixture(t)
	activateStoredProducer(t, e, id, runtime)
	key, _, err := e.AuthorityState.producerKey(runtime)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.authorityBucket.Delete(t.Context(), key); err != nil {
		t.Fatal(err)
	}
	state, err := e.change(t.Context(), "sts", "source", func(l *fencing.Ledger) error { return l.Close("fence", strings.Repeat("a", 64)) })
	if err != nil {
		t.Fatal(err)
	}
	if complete, err := e.closeMutationProducers(t.Context(), state, nil); err == nil || complete {
		t.Fatal("deleted bound journal was fabricated as empty and complete")
	}
	if exists, err := f.authorityBucket.Exists(t.Context(), key); err != nil || exists {
		t.Fatal("missing bound producer recreated")
	}
}

func TestMutationJournalStoreCASRejectsDelayedAdmission(t *testing.T) {
	e, _, id, runtime := newMutationStoreFixture(t)
	activateStoredProducer(t, e, id, runtime)
	prepared := make(chan struct{})
	release := make(chan struct{})
	result := make(chan error, 1)
	go func() {
		first := true
		_, err := e.changeMutationProducer(t.Context(), runtime, func(p *fencing.MutationProducer) error {
			if err := p.Admit(1, completeStoreIntent(fencing.MutationIntent{OperationID: "delayed", Kind: "delete", Key: "key"})); err != nil {
				return err
			}
			if first {
				first = false
				close(prepared)
				<-release
			}
			return nil
		})
		result <- err
	}()
	<-prepared
	state, closeErr := e.change(t.Context(), "sts", "source", func(l *fencing.Ledger) error { return l.Close("fence", strings.Repeat("a", 64)) })
	complete, barrierErr := e.closeMutationProducers(t.Context(), state, nil)
	close(release)
	admitErr := <-result
	if closeErr != nil || barrierErr != nil || !complete || !errors.Is(admitErr, fencing.ErrClosed) {
		t.Fatalf("closure/CAS: close=%v barrier=%v complete=%v admission=%v", closeErr, barrierErr, complete, admitErr)
	}
	p, err := e.reserveMutationProducer(t.Context(), runtime)
	if err != nil || p.LastAdmitted != 0 || !p.BarrierComplete() {
		t.Fatalf("stale admission overwrote durable closure: %+v %v", p, err)
	}
}

func TestMutationJournalStoreLostResponseRetainsSameOperation(t *testing.T) {
	e, f, id, runtime := newMutationStoreFixture(t)
	activateStoredProducer(t, e, id, runtime)
	e.AuthorityState.bucket = &mutationCASReplyFaultBucket{Bucket: f.authorityBucket, loseCASReply: true}
	intent := completeStoreIntent(fencing.MutationIntent{OperationID: "same-op", Kind: "delete", Key: "key"})
	transition := func(p *fencing.MutationProducer) error { return p.Admit(1, intent) }
	if _, err := e.changeMutationProducer(t.Context(), runtime, transition); err != nil {
		t.Fatal(err)
	}
	p, err := e.changeMutationProducer(t.Context(), runtime, transition)
	if err != nil || p.LastAdmitted != 1 || len(p.Entries) != 1 {
		t.Fatalf("uncertain admission duplicated or lost: %+v %v", p, err)
	}
	state, err := e.change(t.Context(), "sts", "source", func(l *fencing.Ledger) error { return l.Close("fence", strings.Repeat("a", 64)) })
	if err != nil {
		t.Fatal(err)
	}
	if complete, err := e.closeMutationProducers(t.Context(), state, nil); err != nil || complete {
		t.Fatalf("unscheduled admitted obligation omitted: %v %v", complete, err)
	}
	_, err = e.changeMutationProducer(t.Context(), runtime, func(p *fencing.MutationProducer) error { return p.CancelUnprepared(1, strings.Repeat("b", 64)) })
	if err != nil {
		t.Fatal(err)
	}
	if complete, err := e.closeMutationProducers(t.Context(), state, nil); err != nil || !complete {
		t.Fatalf("settled operation not retained: %v %v", complete, err)
	}
}

func TestMutationJournalStoreRejectsCorruptionAndBoundsConflicts(t *testing.T) {
	for _, scenario := range []string{"runtime", "corrupt-envelope", "unknown-field", "conflicts", "unenrolled", "closed-without-stop"} {
		t.Run(scenario, func(t *testing.T) {
			e, f, id, runtime := newMutationStoreFixture(t)
			activateStoredProducer(t, e, id, runtime)
			switch scenario {
			case "runtime":
				p, _, version, err := e.AuthorityState.ReadProducer(t.Context(), runtime)
				if err != nil {
					t.Fatal(err)
				}
				p.Runtime.BootID = "other-boot"
				key, identity, err := e.AuthorityState.producerKey(runtime)
				if err != nil {
					t.Fatal(err)
				}
				value := struct {
					ProducerUID string                   `json:"producerUID"`
					Producer    fencing.MutationProducer `json:"producer"`
				}{stableMutationProducerUID(runtime), p}
				if err := e.AuthorityState.write(t.Context(), key, identity, value, version); err != nil {
					t.Fatal(err)
				}
			case "corrupt-envelope":
				key, _, err := e.AuthorityState.producerKey(runtime)
				if err != nil {
					t.Fatal(err)
				}
				if err := f.authorityBucket.Upload(t.Context(), key, strings.NewReader("not an authority envelope")); err != nil {
					t.Fatal(err)
				}
			case "unknown-field":
				key, identity, err := e.AuthorityState.producerKey(runtime)
				if err != nil {
					t.Fatal(err)
				}
				p, _, _, err := e.AuthorityState.ReadProducer(t.Context(), runtime)
				if err != nil {
					t.Fatal(err)
				}
				value := struct {
					ProducerUID string                   `json:"producerUID"`
					Producer    fencing.MutationProducer `json:"producer"`
				}{stableMutationProducerUID(runtime), p}
				data, err := json.Marshal(value)
				if err != nil {
					t.Fatal(err)
				}
				data = append(data[:len(data)-1], []byte(`,"unknown":true}`)...)
				revision, err := newAuthorityRevision()
				if err != nil {
					t.Fatal(err)
				}
				env := authorityStateEnvelope{Version: 1, Identity: identity, Revision: revision, Digest: authorityStateEnvelopeDigest(data, revision), Value: data}
				body, err := json.Marshal(env)
				if err != nil {
					t.Fatal(err)
				}
				if err := f.authorityBucket.Upload(t.Context(), key, bytes.NewReader(body)); err != nil {
					t.Fatal(err)
				}
			case "unenrolled":
				runtime.RunID = "unregistered"
			case "conflicts":
				key, _, err := e.AuthorityState.producerKey(runtime)
				if err != nil {
					t.Fatal(err)
				}
				e.AuthorityState.bucket = &forcedProducerCASConflictBucket{Bucket: f.authorityBucket, key: key, remaining: 8}
			case "closed-without-stop":
				state, version, err := e.AuthorityState.ReadRoot(t.Context(), runtime.StatefulSetUID, runtime.SourceClusterID)
				if err != nil {
					t.Fatal(err)
				}
				state.Ledger.ClosedBy, state.Ledger.ClosedHash = "fence", strings.Repeat("a", 64)
				if err := e.AuthorityState.WriteRoot(t.Context(), runtime.StatefulSetUID, runtime.SourceClusterID, state, version); err != nil {
					t.Fatal(err)
				}
			}
			_, err := e.changeMutationProducer(t.Context(), runtime, func(p *fencing.MutationProducer) error {
				return p.Admit(1, completeStoreIntent(fencing.MutationIntent{OperationID: "op", Kind: "delete", Key: "key"}))
			})
			if err == nil {
				t.Fatal("unsafe producer accepted")
			}
			if scenario == "conflicts" {
				if !errors.Is(err, fencing.ErrUnavailable) {
					t.Fatalf("canonical producer CAS conflicts were not bounded: %v", err)
				}
				p, _, _, readErr := e.AuthorityState.ReadProducer(t.Context(), runtime)
				if readErr != nil || p.LastAdmitted != 0 || len(p.Entries) != 0 {
					t.Fatalf("conflicted admission changed canonical producer: %+v err=%v", p, readErr)
				}
			}
		})
	}
}

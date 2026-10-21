package operator

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/mrchypark/rhiza/pkg/fencing"
)

type mutationStoreFixture struct {
	mu                   sync.Mutex
	objects              map[string]object
	version              int
	conflicts            int
	lostResponse         bool
	lostArchiveResponse  bool
	lostTerminalResponse bool
	lostLedgerResponse   bool
	archiveReads         int
	archivePosts         int
	archiveReadError     string
	statusWrites         int
}

func (f *mutationStoreFixture) serve(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
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
	if r.Method == http.MethodPut && strings.HasSuffix(r.URL.Path, "/status") {
		f.objects[r.URL.Path] = next
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
	w.WriteHeader(200)
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
	e := &FencingExecutor{Kube: &Kubernetes{BaseURL: api.URL, Namespace: "test", TokenFile: token, Client: api.Client()}}
	id := fencing.Identity{SourceClusterID: "source", NodeID: "n0", PodUID: "pod-0", RunID: "run-0"}
	runtime := fencing.RuntimeBinding{
		AuthorityID: "authority", NamespaceUID: "namespace", BindingUID: "binding", StatefulSetUID: "sts",
		SourceClusterID: id.SourceClusterID, NodeID: id.NodeID, PodUID: id.PodUID, RunID: id.RunID,
		NodeName: "node", NodeUID: "node-uid", MachineID: "machine", BootID: "boot", RuntimeID: "containerd", SandboxID: "sandbox", ContainerID: "container", ImageID: "image",
	}
	_, err := e.change(t.Context(), "sts", "source", func(l *fencing.Ledger) error {
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
	restarted := &FencingExecutor{Kube: e.Kube}
	p, err := restarted.reserveMutationProducer(t.Context(), runtime)
	if err != nil || p.Phase != fencing.MutationClosed || !p.BarrierComplete() {
		t.Fatalf("closure not durable: %+v %v", p, err)
	}
	_, err = restarted.changeMutationProducer(t.Context(), runtime, func(p *fencing.MutationProducer) error {
		return p.Admit(1, fencing.MutationIntent{OperationID: "late", Kind: "delete", Key: "key"})
	})
	if !errors.Is(err, fencing.ErrClosed) {
		t.Fatalf("late admission crossed closure: %v", err)
	}
}

func TestMutationJournalStoreMissingBoundStateCannotBeRecreated(t *testing.T) {
	e, f, id, runtime := newMutationStoreFixture(t)
	activateStoredProducer(t, e, id, runtime)
	f.mu.Lock()
	delete(f.objects, e.mutationProducerPath(runtime))
	f.mu.Unlock()
	state, err := e.change(t.Context(), "sts", "source", func(l *fencing.Ledger) error { return l.Close("fence", strings.Repeat("a", 64)) })
	if err != nil {
		t.Fatal(err)
	}
	if complete, err := e.closeMutationProducers(t.Context(), state, nil); err == nil || complete {
		t.Fatal("deleted bound journal was fabricated as empty and complete")
	}
	f.mu.Lock()
	replacement := f.objects[e.mutationProducerPath(runtime)]
	f.mu.Unlock()
	if replacement != nil {
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
			if err := p.Admit(1, fencing.MutationIntent{OperationID: "delayed", Kind: "delete", Key: "key"}); err != nil {
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
	intent := fencing.MutationIntent{OperationID: "same-op", Kind: "delete", Key: "key"}
	f.mu.Lock()
	f.lostResponse = true
	f.mu.Unlock()
	transition := func(p *fencing.MutationProducer) error { return p.Admit(1, intent) }
	if _, err := e.changeMutationProducer(t.Context(), runtime, transition); err == nil {
		t.Fatal("lost response reported as success")
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
	for _, scenario := range []string{"runtime", "deleting", "unknown-field", "missing-uid", "replaced-uid", "conflicts", "unenrolled", "closed-without-stop"} {
		t.Run(scenario, func(t *testing.T) {
			e, f, id, runtime := newMutationStoreFixture(t)
			activateStoredProducer(t, e, id, runtime)
			f.mu.Lock()
			resource := f.objects[e.mutationProducerPath(runtime)]
			switch scenario {
			case "runtime":
				var p fencing.MutationProducer
				_ = json.Unmarshal([]byte(str(nested(resource, "data", "producer.json"))), &p)
				p.Runtime.BootID = "other-boot"
				data, _ := json.Marshal(p)
				asObject(resource["data"])["producer.json"] = string(data)
			case "deleting":
				asObject(resource["metadata"])["deletionTimestamp"] = "now"
			case "unknown-field":
				data := str(nested(resource, "data", "producer.json"))
				asObject(resource["data"])["producer.json"] = strings.TrimSuffix(data, "}") + `,"compactedThrough":100}`
			case "missing-uid":
				delete(asObject(resource["metadata"]), "uid")
			case "replaced-uid":
				asObject(resource["metadata"])["uid"] = "replacement-object-uid"
			case "conflicts":
				f.conflicts = 8
			case "unenrolled":
				runtime.RunID = "unregistered"
			case "closed-without-stop":
				root := f.objects[e.ledgerPath(runtime.StatefulSetUID, runtime.SourceClusterID)]
				var state fenceLedger
				_ = json.Unmarshal([]byte(str(nested(root, "data", "ledger.json"))), &state)
				state.Ledger.ClosedBy, state.Ledger.ClosedHash = "fence", strings.Repeat("a", 64)
				data, _ := json.Marshal(state)
				asObject(root["data"])["ledger.json"] = string(data)
			}
			f.mu.Unlock()
			_, err := e.changeMutationProducer(t.Context(), runtime, func(p *fencing.MutationProducer) error {
				return p.Admit(1, fencing.MutationIntent{OperationID: "op", Kind: "delete", Key: "key"})
			})
			if err == nil {
				t.Fatal("unsafe producer accepted")
			}
			if scenario == "conflicts" && !errors.Is(err, fencing.ErrUnavailable) {
				t.Fatalf("conflict retry not bounded: %v", err)
			}
		})
	}
}

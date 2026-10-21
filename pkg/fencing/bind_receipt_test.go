package fencing

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
)

func testProducerReceipt() BindReceipt {
	id := testIdentity()
	id.WALIdentity = "wal"
	return BindReceipt{Identity: id, Producer: &ProducerBinding{ContractVersion: MutationIntentVersion,
		Runtime: enrolledRuntime(id, "container"), ProducerUID: "pinned-producer",
		StoreScopeHash: journalTestDigest, ExecutionProfile: MutationExecutionProfile}}
}

func TestProducerBindReceiptRejectsIncompleteOrDifferentIdentity(t *testing.T) {
	original := testProducerReceipt()
	mutations := map[string]func(*BindReceipt){
		"identity": func(r *BindReceipt) { r.Identity.WALIdentity = "other" },
		"runtime":  func(r *BindReceipt) { r.Producer.Runtime.RunID = "other" },
		"producer": func(r *BindReceipt) { r.Producer.ProducerUID = "" },
		"version":  func(r *BindReceipt) { r.Producer.ContractVersion = 0 },
		"scope":    func(r *BindReceipt) { r.Producer.StoreScopeHash = "" },
		"profile":  func(r *BindReceipt) { r.Producer.ExecutionProfile = "unsupported" },
	}
	for name, change := range mutations {
		t.Run(name, func(t *testing.T) {
			r := original.clone()
			change(&r)
			if r.Validate(original.Identity) == nil {
				t.Fatal("invalid bind receipt accepted")
			}
		})
	}
	mustJournal(t, original.Validate(original.Identity))
}

func TestSessionBindingLostResponseAndChangedProducer(t *testing.T) {
	original := testProducerReceipt()
	start := original.Identity
	start.WALIdentity = ""
	calls := 0
	reply := original.clone()
	s := &Session{identity: start, client: &http.Client{Transport: sessionRoundTrip(func(r *http.Request) (*http.Response, error) {
		calls++
		var submitted Identity
		if err := json.NewDecoder(r.Body).Decode(&submitted); err != nil || submitted != original.Identity {
			t.Errorf("retry changed original binding: %+v %v", submitted, err)
		}
		if calls == 1 {
			return &http.Response{StatusCode: http.StatusServiceUnavailable, Body: io.NopCloser(strings.NewReader(""))}, nil
		}
		data, err := json.Marshal(reply)
		if err != nil {
			return nil, err
		}
		return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(bytes.NewReader(data))}, nil
	})}}
	if err := s.BindWAL(t.Context(), "wal"); !errors.Is(err, ErrUnavailable) || s.Identity() != start {
		t.Fatalf("uncertain response enabled source: %v", err)
	}
	if _, enabled := s.MutationBinding(); enabled {
		t.Fatal("client enabled before validated receipt")
	}
	mustJournal(t, s.BindWAL(t.Context(), "wal"))
	bound, enabled := s.MutationBinding()
	if !enabled || bound != *original.Producer {
		t.Fatal("original producer binding was not recovered")
	}
	bound.ProducerUID = "caller-mutated-copy"
	reply.Producer.ProducerUID = "replacement"
	if err := s.BindWAL(t.Context(), "wal"); err == nil {
		t.Fatal("same WAL switched producer UID")
	}
	still, _ := s.MutationBinding()
	if still != *original.Producer {
		t.Fatal("rejected response or caller copy changed pinned binding")
	}
	reply = BindReceipt{Identity: original.Identity}
	if err := s.BindWAL(t.Context(), "wal"); err == nil {
		t.Fatal("mutation binding silently downgraded to cooperative")
	}
}

func TestSessionBindRejectsLegacyEmptyResponse(t *testing.T) {
	start := testProducerReceipt().Identity
	start.WALIdentity = ""
	for _, body := range []string{"", "{}"} {
		t.Run(body, func(t *testing.T) {
			s := &Session{identity: start, client: &http.Client{Transport: sessionRoundTrip(func(*http.Request) (*http.Response, error) {
				return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(body))}, nil
			})}}
			if err := s.BindWAL(t.Context(), "wal"); err == nil {
				t.Fatal("empty legacy response enabled WAL binding")
			}
			if s.Identity() != start {
				t.Fatal("rejected response changed identity")
			}
			if _, enabled := s.MutationBinding(); enabled {
				t.Fatal("rejected response enabled mutation client")
			}
		})
	}
}

func TestProducerScopeCannotBeRefreshedAfterBinding(t *testing.T) {
	id := testIdentity()
	id.WALIdentity = ""
	l := Ledger{SourceClusterID: id.SourceClusterID, RequireMutationJournal: true}
	runtime := enrolledRuntime(id, "container")
	mustJournal(t, l.RegisterEnrolled(id, runtime))
	mustJournal(t, l.BindMutationProducer(id, runtime, "producer"))
	id.WALIdentity = "wal"
	mustJournal(t, l.BindMutationScope(id, journalTestDigest, MutationExecutionProfile))
	mustJournal(t, l.Bind(id))
	data, err := json.Marshal(l)
	mustJournal(t, err)
	var restarted Ledger
	mustJournal(t, json.Unmarshal(data, &restarted))
	mustJournal(t, restarted.BindMutationScope(id, journalTestDigest, MutationExecutionProfile))
	before, err := json.Marshal(restarted)
	mustJournal(t, err)
	if restarted.BindMutationScope(id, strings.Repeat("b", 64), MutationExecutionProfile) == nil {
		t.Fatal("restart refreshed original scope")
	}
	id.WALIdentity = "other"
	if restarted.BindMutationScope(id, journalTestDigest, MutationExecutionProfile) == nil {
		t.Fatal("scope binding accepted another WAL")
	}
	after, err := json.Marshal(restarted)
	mustJournal(t, err)
	if !bytes.Equal(before, after) {
		t.Fatal("rejected binding changed durable state")
	}
}

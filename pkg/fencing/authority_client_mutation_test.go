package fencing

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestAuthorityClientMutationLookupPreservesExactTuple(t *testing.T) {
	intent := MutationIntent{
		ContractVersion: MutationIntentVersion, StoreScopeHash: strings.Repeat("a", 64),
		OwnerStep: MutationOwnerStep{Owner: "owner", Workflow: "workflow", Step: "step", Attempt: 1},
		Condition: MutationCondition{Mode: MutationUnconditional}, OperationID: MutationRequestID("producer", 1),
		Kind: "upload", Key: "key", PayloadRef: "private/object", PayloadDigest: strings.Repeat("b", 64), PayloadSize: 3,
	}
	entry := MutationEntry{Intent: intent}
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.Header.Get("Authorization") != "Bearer token" {
			t.Errorf("request auth/method mismatch: %s %q", r.Method, r.Header.Get("Authorization"))
		}
		var message AuthorityMessage
		if err := json.NewDecoder(r.Body).Decode(&message); err != nil {
			t.Fatal(err)
		}
		if message.Action != "mutation-lookup" || message.Identity.WALIdentity != "wal" || message.MutationLookup == nil || message.MutationLookup.ProducerUID != "producer" || message.MutationLookup.Sequence != 1 || message.MutationLookup.Intent != intent {
			t.Errorf("lookup request changed: %+v", message)
		}
		_ = json.NewEncoder(w).Encode(MutationLookupResult{Found: true, Entry: &entry})
	}))
	defer server.Close()

	token := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(token, []byte("token"), 0600); err != nil {
		t.Fatal(err)
	}
	client := AuthorityClient{URL: server.URL, TokenFile: token, Client: server.Client()}
	id := Identity{SourceClusterID: "source", NodeID: "node", PodUID: "pod", RunID: "run", WALIdentity: "wal"}
	got, err := client.LookupMutation(context.Background(), id, "producer", 1, intent)
	if err != nil || !got.Found || got.Entry == nil || got.Entry.Intent != intent {
		t.Fatalf("lookup lost original entry: %+v %v", got, err)
	}
	changed := intent
	changed.PayloadDigest = strings.Repeat("c", 64)
	if _, err := client.LookupMutation(context.Background(), id, "producer", 1, changed); err == nil {
		t.Fatal("changed operation identity accepted locally")
	}
}

func TestAuthorityClientMutationLookupRejectsTupleChangedByAuthority(t *testing.T) {
	intent := MutationIntent{
		ContractVersion: MutationIntentVersion, StoreScopeHash: strings.Repeat("a", 64),
		OwnerStep: MutationOwnerStep{Owner: "owner", Workflow: "workflow", Step: "step", Attempt: 1},
		Condition: MutationCondition{Mode: MutationUnconditional}, OperationID: MutationRequestID("producer", 1),
		Kind: "upload", Key: "key", PayloadRef: "private/object", PayloadDigest: strings.Repeat("b", 64), PayloadSize: 3,
	}
	changed := intent
	changed.PayloadDigest = strings.Repeat("c", 64)
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(MutationLookupResult{Found: true, Entry: &MutationEntry{Intent: changed}})
	}))
	defer server.Close()
	token := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(token, []byte("token"), 0600); err != nil {
		t.Fatal(err)
	}
	client := AuthorityClient{URL: server.URL, TokenFile: token, Client: server.Client()}
	id := Identity{SourceClusterID: "source", NodeID: "node", PodUID: "pod", RunID: "run", WALIdentity: "wal"}
	if _, err := client.LookupMutation(context.Background(), id, "producer", 1, intent); err == nil {
		t.Fatal("authority tuple mismatch accepted")
	}
}

func TestAuthorityClientMutationLookupRejectsMalformedResolution(t *testing.T) {
	intent := MutationIntent{
		ContractVersion: MutationIntentVersion, StoreScopeHash: strings.Repeat("a", 64),
		OwnerStep: MutationOwnerStep{Owner: "owner", Workflow: "workflow", Step: "step", Attempt: 1},
		Condition: MutationCondition{Mode: MutationUnconditional}, OperationID: MutationRequestID("producer", 1),
		Kind: "upload", Key: "key", PayloadRef: "private/object", PayloadDigest: strings.Repeat("b", 64), PayloadSize: 3,
	}
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		entry := MutationEntry{Intent: intent, Resolution: &MutationResolution{Outcome: "success", ReceiptDigest: strings.Repeat("c", 64)}}
		_ = json.NewEncoder(w).Encode(MutationLookupResult{Found: true, Entry: &entry})
	}))
	defer server.Close()
	token := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(token, []byte("token"), 0600); err != nil {
		t.Fatal(err)
	}
	client := AuthorityClient{URL: server.URL, TokenFile: token, Client: server.Client()}
	id := Identity{SourceClusterID: "source", NodeID: "node", PodUID: "pod", RunID: "run", WALIdentity: "wal"}
	if _, err := client.LookupMutation(context.Background(), id, "producer", 1, intent); err == nil {
		t.Fatal("malformed resolution accepted")
	}
}

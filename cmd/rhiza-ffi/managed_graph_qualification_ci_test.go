//go:build cgo && rhiza_local_testhooks

package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/localtesthooks"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/managedsql"
	"github.com/mrchypark/rhiza/pkg/network"
)

type graphQualificationRoundTrip func(*http.Request) (*http.Response, error)

func (f graphQualificationRoundTrip) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

// Runs in the real Pod after Rust has closed its Handle. These counters belong
// to this Go C-ABI Host, NOT the earlier separate Rust process.
func TestManagedFFIInClusterGraphQualification(t *testing.T) {
	if os.Getenv("RHIZA_FFI_GRAPH_QUALIFICATION") != "1" {
		t.Skip("requires actual in-cluster Rust tuple/product publisher; no local substitute")
	}
	uid := os.Getenv("POD_UID")
	if len(uid) < 8 {
		t.Fatal("actual Pod identity unavailable")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	var clients, totalKube, normalKube, proposals atomic.Int64
	var normal atomic.Bool
	restoreClient := localtesthooks.SetManagedKubeClient(func(client *http.Client) {
		clients.Add(1)
		base := client.Transport
		if base == nil {
			base = http.DefaultTransport
		}
		client.Transport = graphQualificationRoundTrip(func(r *http.Request) (*http.Response, error) {
			totalKube.Add(1)
			if normal.Load() {
				normalKube.Add(1)
			}
			return base.RoundTrip(r)
		})
	})
	defer restoreClient()
	var opened struct {
		Data struct {
			Handle uint64 `json:"handle"`
		} `json:"data"`
		Error *ffiError `json:"error"`
	}
	if json.Unmarshal(goOpenManaged([]byte(`{}`)), &opened) != nil || opened.Error != nil || opened.Data.Handle == 0 {
		t.Fatal("actual Go C-ABI managed open failed")
	}
	handle := opened.Data.Handle
	closed := false
	t.Cleanup(func() {
		if !closed {
			_ = goClose(handle)
		}
	})
	if clients.Load() != 1 || totalKube.Load() == 0 {
		t.Fatal("actual managed Kube client observer was not connected before startup")
	}
	invoke := func(operation string, request any) json.RawMessage {
		t.Helper()
		wire, err := json.Marshal(map[string]any{"operation": operation, "request": request})
		if err != nil {
			t.Fatal("encode qualification call")
		}
		var response struct {
			Data  json.RawMessage `json:"data"`
			Error *ffiError       `json:"error"`
		}
		if json.Unmarshal(goCall(handle, wire, 30000), &response) != nil || response.Error != nil {
			t.Fatalf("actual qualification call failed operation=%s", operation)
		}
		return response.Data
	}
	tick := time.NewTicker(100 * time.Millisecond)
	defer tick.Stop()
	for {
		var ready bool
		if json.Unmarshal(invoke("ready", struct{}{}), &ready) == nil && ready {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("actual public Ready did not complete")
		case <-tick.C:
		}
	}
	entry, release, err := acquire(handle)
	if err != nil {
		t.Fatal("borrow actual managed Handle")
	}
	if entry.managed == nil || entry.db != nil {
		release()
		t.Fatal("actual Handle is not managed")
	}
	defer func() { release() }()
	// Observe the product worker's actual root, outside the normal request
	// measurement window. Never call a test-only force publisher.
	var first managedsql.ManagedGraphQualificationObservation
	for {
		first, err = entry.managed.ObserveManagedGraphQualification(ctx, "", nil)
		if err == nil {
			break
		}
		if !errors.Is(err, network.ErrNotReady) {
			t.Fatal("actual product paired root validation failed")
		}
		select {
		case <-ctx.Done():
			t.Fatal("actual product paired root never became verifiable")
		case <-tick.C:
		}
	}
	var rows struct {
		Rows [][]any `json:"rows"`
	}
	if json.Unmarshal(invoke("query", map[string]any{"sql": "SELECT tuple FROM rust_graph_contract WHERE id=1", "consistency": "linearizable"}), &rows) != nil || len(rows.Rows) != 1 || len(rows.Rows[0]) != 1 {
		t.Fatal("actual Rust tuple unavailable")
	}
	text, ok := rows.Rows[0][0].(string)
	if !ok || len(text) > 32768 {
		t.Fatal("actual Rust tuple malformed")
	}
	var tuple struct {
		Version int `json:"version"`
		Cases   []struct {
			ID       []byte                `json:"request_id_b64"`
			Original []byte                `json:"expected_original_b64"`
			Frame    []byte                `json:"frame_b64"`
			Receipt  types.MutationReceipt `json:"receipt"`
		} `json:"cases"`
	}
	if json.Unmarshal([]byte(text), &tuple) != nil || tuple.Version != 1 || len(tuple.Cases) != 5 {
		t.Fatal("actual Rust case set malformed")
	}
	startupKube := totalKube.Load()
	normalKube.Store(0)
	normal.Store(true)
	restoreProposal := localtesthooks.SetProposalEntry(func(value []byte) {
		if managedsql.QualificationApplicationKind(value) != "" {
			proposals.Add(1)
		}
	})
	defer restoreProposal()
	command := types.GraphCommand{RequestID: fmt.Sprintf("go-q-%s-%d", uid[:8], time.Now().UnixNano()), Cypher: "CREATE (:QualificationCI {value:1})"}
	frame, err := types.EncodeManagedGraphExecution(command)
	if err != nil {
		t.Fatal("encode actual suffix command")
	}
	goOriginal, err := types.EncodeManagedGraphOriginal(command)
	if err != nil {
		t.Fatal("encode suffix original")
	}
	var suffix types.MutationReceipt
	if json.Unmarshal(invoke("graph_execute", map[string]any{"frame_b64": frame}), &suffix) != nil || suffix.Slot <= first.Archive.Locator.SealDecisionSlot || suffix.Status != types.MutationCommitted || !suffix.Applied {
		t.Fatal("actual Graph suffix ACK missing")
	}
	positive := proposals.Load()
	if positive == 0 {
		t.Fatal("Core observer missed the actual positive Graph proposal")
	}
	kvWire, _, err := types.EncodeManagedKVOriginal(types.KVCommand{RequestID: command.RequestID + "-kv", Operation: "put", Key: "qualification-observer", Value: []byte("positive")})
	if err != nil {
		t.Fatal("encode KV positive control")
	}
	var kvReceipt types.MutationReceipt
	if json.Unmarshal(invoke("kv_put", kvWire), &kvReceipt) != nil || kvReceipt.Slot == 0 || kvReceipt.Status != types.MutationCommitted || !kvReceipt.Applied {
		t.Fatal("actual KV positive ACK missing")
	}
	kvPositive := proposals.Load() - positive
	if kvPositive <= 0 {
		t.Fatal("observer missed actual KV positive entry")
	}
	var sqlReceipt types.MutationReceipt
	if json.Unmarshal(invoke("execute", map[string]any{"request_id": command.RequestID + "-sql", "sql": "CREATE TABLE IF NOT EXISTS qualification_observer(n INTEGER)"}), &sqlReceipt) != nil || sqlReceipt.Slot == 0 || sqlReceipt.Status != types.MutationCommitted {
		t.Fatal("actual SQL positive ACK missing")
	}
	sqlPositive := proposals.Load() - positive - kvPositive
	if sqlPositive <= 0 {
		t.Fatal("observer missed actual SQL positive entry")
	}
	proposals.Store(0)
	for _, c := range tuple.Cases {
		if len(c.ID) == 0 || c.Receipt.Slot == 0 || c.Receipt.Status != types.MutationCommitted || !c.Receipt.Applied {
			t.Fatal("Rust case is not an ACK")
		}
		observation, err := entry.managed.ObserveManagedGraphQualification(ctx, string(c.ID), &first.Archive.Locator)
		if err != nil || !observation.OriginalFound {
			t.Fatal("actual native original observation failed")
		}
		original := observation.Original
		if original.Graph == nil || original.RequestID != string(c.ID) || original.Slot != c.Receipt.Slot || !bytes.Equal(original.CanonicalOriginal, c.Original) || !bytes.Equal(original.ExecutionItem, c.Frame) || original.DecisionHash != sha256.Sum256(c.Frame) || original.Receipt != c.Receipt {
			t.Fatal("actual native canonical original/receipt differs from Rust ACK")
		}
		var recovered struct {
			Kind  string                 `json:"kind"`
			ID    []byte                 `json:"request_id_b64"`
			Graph *types.MutationReceipt `json:"graph"`
		}
		if json.Unmarshal(invoke("recover_pending_detailed", map[string]any{"kind": "graph", "request_id_b64": c.ID}), &recovered) != nil || recovered.Kind != "graph" || !bytes.Equal(recovered.ID, c.ID) || recovered.Graph == nil || *recovered.Graph != c.Receipt {
			t.Fatal("actual C-ABI exact recovery changed Rust receipt")
		}
	}
	last, err := entry.managed.ObserveManagedGraphQualification(ctx, command.RequestID, &first.Archive.Locator)
	if err != nil || !last.OriginalFound || last.Original.Slot != suffix.Slot || !bytes.Equal(last.Original.CanonicalOriginal, goOriginal) || !bytes.Equal(last.Original.ExecutionItem, frame) || last.Original.DecisionHash != sha256.Sum256(frame) || last.Archive.Base != first.Archive.Base || last.Archive.Tip < suffix.Slot || last.Archive.GraphSuffixDecisions < 1 {
		t.Fatal("actual product paired base/nonempty certified Graph suffix unavailable")
	}
	if !last.StatsAvailable || last.Stats.HTTPRequests == 0 || last.Stats.Deletes != 0 || last.Stats.HTTPDeleteRequests != 0 {
		t.Fatal("actual SDK Delete/HTTP observation failed")
	}
	if proposals.Load() != 0 || normalKube.Load() != 0 {
		t.Fatalf("normal C-ABI recovery touched forbidden path: application_proposals=%d kube=%d", proposals.Load(), normalKube.Load())
	}
	normal.Store(false)
	// Drop the borrowed activity before Close, so Close can actually drain.
	release()
	release = func() {}
	var closeResponse struct {
		Error *ffiError `json:"error"`
	}
	if json.Unmarshal(goClose(handle), &closeResponse) != nil || closeResponse.Error != nil {
		t.Fatal("actual qualification Host close failed")
	}
	closed = true
	marker := struct {
		Version           int    `json:"version"`
		Scope             string `json:"scope"`
		PodUID            string `json:"pod_uid"`
		Base              uint64 `json:"base"`
		Tip               uint64 `json:"tip"`
		RootHash          string `json:"root_hash"`
		SealSlot          uint64 `json:"seal_slot"`
		SealHash          string `json:"seal_hash"`
		GraphSuffix       int    `json:"graph_suffix"`
		NativeOriginals   int    `json:"native_originals"`
		Kube              int64  `json:"normal_kube_requests"`
		RecoveryProposals int64  `json:"recovery_application_proposal_entries"`
		PositiveProposals int64  `json:"positive_graph_proposal_entries"`
		PositiveKV        int64  `json:"positive_kv_proposal_entries"`
		PositiveSQL       int64  `json:"positive_sql_proposal_entries"`
		LogicalDeletes    uint64 `json:"logical_deletes"`
		HTTPDeletes       uint64 `json:"http_deletes"`
		SDKRequests       uint64 `json:"sdk_http_requests"`
		StartupKube       int64  `json:"startup_kube_requests"`
	}{Version: 1, Scope: "go-cabi-incluster", PodUID: uid, Base: last.Archive.Base, Tip: last.Archive.Tip, RootHash: fmt.Sprintf("%x", last.Archive.Locator.RootHash), SealSlot: last.Archive.Locator.SealDecisionSlot, SealHash: fmt.Sprintf("%x", last.Archive.Locator.SealDecisionHash), GraphSuffix: last.Archive.GraphSuffixDecisions, NativeOriginals: len(tuple.Cases), Kube: normalKube.Load(), RecoveryProposals: proposals.Load(), PositiveProposals: positive, PositiveKV: kvPositive, PositiveSQL: sqlPositive, LogicalDeletes: last.Stats.Deletes, HTTPDeletes: last.Stats.HTTPDeleteRequests, SDKRequests: last.Stats.HTTPRequests, StartupKube: startupKube}
	encoded, err := json.Marshal(marker)
	if err != nil {
		t.Fatal("encode bounded qualification marker")
	}
	fmt.Printf("GRAPH_CABI_QUALIFICATION=%s\n", encoded)
	fmt.Println("PASS: actual Go CABI Graph native/root/counter qualification")
}

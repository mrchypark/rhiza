package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"cloud.google.com/go/storage"
	"github.com/mrchypark/rhiza/pkg/fencing"
	"github.com/mrchypark/rhiza/pkg/operator"
	"google.golang.org/api/option"
)

func TestWaitForGCSAckLossReceiptParsesExactlyOneStrictPrivateRecord(t *testing.T) {
	tmp := t.TempDir()
	logPath := filepath.Join(tmp, "fencer.log")
	kubectlPath := filepath.Join(tmp, "kubectl")
	if err := os.WriteFile(kubectlPath, []byte("#!/bin/sh\nexec cat '"+strings.ReplaceAll(logPath, "'", "'\"'\"'")+"'\n"), 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", tmp+string(os.PathListSeparator)+os.Getenv("PATH"))
	valid, err := json.Marshal(operator.GCSMutationAckLossReceipt{Version: 1, Injected: true, WireRequests: 1, OperationID: "private-operation"})
	if err != nil {
		t.Fatal(err)
	}
	prefix := gcsAckLossEventPrefix
	for _, test := range []struct {
		name    string
		log     string
		wantErr bool
	}{
		{name: "one exact receipt", log: prefix + string(valid) + "\n"},
		{name: "duplicate receipt", log: prefix + string(valid) + "\n" + prefix + string(valid) + "\n", wantErr: true},
		{name: "unknown field", log: prefix + `{"version":1,"injected":true,"wireRequests":1,"operationID":"private-operation","extra":true}` + "\n", wantErr: true},
		{name: "trailing JSON", log: prefix + string(valid) + ` {"second":true}` + "\n", wantErr: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			out := filepath.Join(tmp, strings.ReplaceAll(test.name, " ", "-"))
			if err := os.Mkdir(out, 0700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(logPath, []byte(test.log), 0600); err != nil {
				t.Fatal(err)
			}
			receipt, err := waitForGCSAckLossReceipt(context.Background(), "fencer", out)
			if test.wantErr {
				if err == nil {
					t.Fatal("malformed or ambiguous fencer log was accepted")
				}
				return
			}
			if err != nil || receipt.OperationID != "private-operation" || receipt.WireRequests != 1 {
				t.Fatalf("valid receipt parse failed: receipt=%+v err=%v", receipt, err)
			}
			info, err := os.Stat(filepath.Join(out, gcsAckLossReceiptName))
			if err != nil || info.Mode().Perm() != 0600 {
				t.Fatalf("private receipt was not saved with mode 0600: info=%v err=%v", info, err)
			}
		})
	}
}

func TestVerifyGCSAckLossHeldObjectRequiresExactHeldGenerationAndBytes(t *testing.T) {
	payload := []byte("sealed archive extent bytes")
	digest := sha256.Sum256(payload)
	payloadDigest := hex.EncodeToString(digest[:])
	for _, test := range []struct {
		name              string
		objectKey         string
		generation        string
		metageneration    string
		size              string
		held              bool
		bytes             []byte
		dropHoldAfterRead bool
		wantErr           bool
	}{
		{name: "exact held object", generation: "41", metageneration: "3", size: strconv.Itoa(len(payload)), held: true, bytes: payload},
		{name: "wrong generation", generation: "42", metageneration: "3", size: strconv.Itoa(len(payload)), held: true, bytes: payload, wantErr: true},
		{name: "wrong metageneration", generation: "41", metageneration: "4", size: strconv.Itoa(len(payload)), held: true, bytes: payload, wantErr: true},
		{name: "not held", generation: "41", metageneration: "3", size: strconv.Itoa(len(payload)), held: false, bytes: payload, wantErr: true},
		{name: "wrong size", generation: "41", metageneration: "3", size: "1", held: true, bytes: payload, wantErr: true},
		{name: "wrong bytes", generation: "41", metageneration: "3", size: strconv.Itoa(len(payload)), held: true, bytes: []byte("different extent"), wantErr: true},
		{name: "hold changed during read", generation: "41", metageneration: "3", size: strconv.Itoa(len(payload)), held: true, bytes: payload, dropHoldAfterRead: true, wantErr: true},
		{name: "noncanonical archive key", objectKey: "archive/head.bin", generation: "41", metageneration: "3", size: strconv.Itoa(len(payload)), held: true, bytes: payload, wantErr: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			objectKey := test.objectKey
			if objectKey == "" {
				objectKey = "archive/blocks/" + strings.Repeat("a", 64) + "_00000000000000000001.bin"
			}
			objectName := path.Join("cluster/source", objectKey)
			metadataReads := 0
			mediaReads := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodGet || r.URL.Query().Get("generation") != "41" {
					http.Error(w, "wrong exact-object request", http.StatusBadRequest)
					return
				}
				if r.URL.Query().Get("alt") == "media" {
					if r.URL.Path != "/ack-bucket/"+objectName {
						http.Error(w, "wrong exact-generation media route", http.StatusBadRequest)
						return
					}
					mediaReads++
					_, _ = w.Write(test.bytes)
					return
				}
				if !strings.Contains(r.URL.Path, "/b/ack-bucket/o/") || !strings.HasSuffix(r.URL.Path, objectName) {
					http.Error(w, "wrong exact-object request", http.StatusBadRequest)
					return
				}
				metadataReads++
				held := test.held
				if test.dropHoldAfterRead && metadataReads > 1 {
					held = false
				}
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(map[string]any{
					"kind": "storage#object", "bucket": "ack-bucket", "name": objectName,
					"generation": test.generation, "metageneration": test.metageneration, "size": test.size,
					"temporaryHold": held, "metadata": map[string]string{"rhiza-operation-id": "ack-op", "rhiza-payload-sha256": payloadDigest},
				})
			}))
			defer server.Close()
			client, err := storage.NewClient(context.Background(), option.WithEndpoint(server.URL+"/storage/v1/"), option.WithHTTPClient(server.Client()), option.WithoutAuthentication())
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()
			source := gcsRecoverySource{Bucket: "ack-bucket", Prefix: "cluster/source"}
			receipt := operator.GCSMutationAckLossReceipt{Runtime: fencing.RuntimeBinding{RunID: "run"}, Intent: fencing.MutationIntent{PayloadSize: int64(len(payload))},
				OperationID: "ack-op", ObjectKey: objectKey, PayloadDigest: payloadDigest, PayloadSize: int64(len(payload)),
				Preparation: fencing.MutationPreparation{GenerationSet: true, Generation: 0}}
			observation := gcsHAAckObservation{Runtime: receipt.Runtime, Guarded: fencing.MutationGuardedLogicalResolution{
				ObjectGeneration: 41, ObjectMetageneration: 3, ObjectSize: int64(len(payload)), PayloadDigest: payloadDigest}}
			got, err := verifyGCSAckLossHeldObject(context.Background(), client, source, receipt, observation)
			if test.wantErr {
				if err == nil {
					t.Fatalf("invalid held-object case was accepted: %+v", got)
				}
				return
			}
			if err != nil || got.Generation != 41 || got.Metageneration != 3 || got.Size != int64(len(payload)) || got.PayloadSHA256 != payloadDigest || !got.TemporaryHold {
				t.Fatalf("exact held object was not verified: evidence=%+v err=%v", got, err)
			}
			if mediaReads != 1 {
				t.Fatalf("expected exactly one pinned-SDK XML media read for the selected generation, got %d", mediaReads)
			}
		})
	}
}

func TestProjectGCSAckLossEvidenceKeepsReceiptAndRuntimePrivate(t *testing.T) {
	raw, public := t.TempDir(), filepath.Join(t.TempDir(), "public")
	if err := os.Mkdir(public, 0700); err != nil {
		t.Fatal(err)
	}
	secret := "private-pod-uid private-object-key private-bucket"
	if err := os.WriteFile(filepath.Join(raw, gcsAckLossReceiptName), []byte(secret), 0600); err != nil {
		t.Fatal(err)
	}
	var events strings.Builder
	for _, name := range []string{"ack_loss_fault_armed", "ack_loss_canonical_and_object_verified", "ack_loss_two_recoveries_complete"} {
		fmt.Fprintf(&events, `{"event":%q,"time":"2026-10-23T00:00:00Z","data":{}}`+"\n", name)
	}
	if err := os.WriteFile(filepath.Join(raw, gcsAckLossEventsName), []byte(events.String()), 0600); err != nil {
		t.Fatal(err)
	}
	seen, err := projectGCSAckLossEvidence(raw, public, true)
	if err != nil || !seen {
		t.Fatalf("failed-run private evidence projection failed: seen=%v err=%v", seen, err)
	}
	entries, err := os.ReadDir(public)
	if err != nil || len(entries) != 1 || entries[0].Name() != gcsAckLossSummaryName {
		t.Fatalf("public projection included non-summary content: entries=%v err=%v", entries, err)
	}
	data, err := os.ReadFile(filepath.Join(public, gcsAckLossSummaryName))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(data), secret) || strings.Contains(string(data), "private-pod-uid") || strings.Contains(string(data), "private-object-key") {
		t.Fatal("public summary disclosed private receipt identity or object location")
	}
}

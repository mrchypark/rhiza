//go:build rhiza_latency_diag

package network

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/latencydiag"
	"github.com/mrchypark/rhiza/pkg/materializer"
	"github.com/mrchypark/rhiza/pkg/qlog"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

func TestTaggedHTTPCarriesSendReceiveAdmissionAndSlot(t *testing.T) {
	const child = "RHIZA_TAGGED_HTTP_CHILD"
	if os.Getenv(child) == "1" {
		latencydiag.DisableBackgroundWriterForTest()
		latencydiag.SetLosslessForTest(true)
		slot := testTaggedHTTPTrace(t)
		fmt.Fprintf(os.Stderr, "tagged_http_response_slot=%d\n", slot)
		latencydiag.Flush()
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestTaggedHTTPCarriesSendReceiveAdmissionAndSlot$")
	cmd.Env = append(os.Environ(), child+"=1")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("tagged HTTP fixture: %v\n%s", err, out)
	}
	if strings.Contains(string(out), "tagged-http") || strings.Contains(string(out), "INSERT INTO") {
		t.Fatalf("tagged trace contains request content: %s", out)
	}
	type observedEvent struct{ slot, outcome, status string }
	events := make(map[string]observedEvent)
	var responseSlot uint64
	for _, line := range strings.Split(string(out), "\n") {
		stage, tag, slot, outcome, status := "", "", "", "", ""
		for _, field := range strings.Fields(line) {
			switch {
			case strings.HasPrefix(field, "stage="):
				stage = strings.TrimPrefix(field, "stage=")
			case strings.HasPrefix(field, "tag="):
				tag = strings.TrimPrefix(field, "tag=")
			case strings.HasPrefix(field, "slot="):
				slot = strings.TrimPrefix(field, "slot=")
			case strings.HasPrefix(field, "outcome="):
				outcome = strings.TrimPrefix(field, "outcome=")
			case strings.HasPrefix(field, "count="):
				status = strings.TrimPrefix(field, "count=")
			}
		}
		if value, ok := strings.CutPrefix(line, "tagged_http_response_slot="); ok {
			responseSlot, _ = strconv.ParseUint(strings.TrimSpace(value), 10, 64)
			continue
		}
		if tag == "" || tag == "0" || stage == "" {
			continue
		}
		key := tag + "/" + stage
		if _, exists := events[key]; exists {
			t.Fatalf("duplicate tagged event %s", key)
		}
		events[key] = observedEvent{slot: slot, outcome: outcome, status: status}
	}
	for _, stage := range []string{
		"http_receive", "mutation_entry", "mutation_admission",
		latencydiag.StagePreEnqueueClone, latencydiag.StagePreEnqueueValidate,
		latencydiag.StagePreEnqueueFingerprint, latencydiag.StagePreEnqueueEncode,
		latencydiag.StageRequestLockWait, latencydiag.StageRequestLockHold,
		latencydiag.StageMaterializerReceipt, latencydiag.StageBatcherEnqueue,
		"batch_proposal", "batch_result", "http_response",
	} {
		if _, found := events["16/"+stage]; !found {
			t.Errorf("missing %s event for tag 16", stage)
		}
	}
	for _, stage := range []string{latencydiag.StageRequestLockWait, latencydiag.StageRequestLockHold} {
		if event := events["16/"+stage]; event.status == "0" {
			t.Errorf("%s did not retain a request-lock shard", stage)
		}
	}
	if responseSlot == 0 {
		t.Fatal("HTTP response did not include a nonzero slot")
	}
	for _, stage := range []string{"batch_proposal", "batch_result"} {
		if got := events["16/"+stage].slot; got != strconv.FormatUint(responseSlot, 10) {
			t.Errorf("%s slot=%s, HTTP response slot=%d", stage, got, responseSlot)
		}
	}
	if event := events["16/http_response"]; event.outcome != "ok" || event.status != "200" {
		t.Errorf("success HTTP outcome=%q status=%q", event.outcome, event.status)
	}
	if event := events["32/http_response"]; event.outcome != "error" || event.status != "400" {
		t.Errorf("failure HTTP outcome=%q status=%q", event.outcome, event.status)
	}
	if _, exists := events["32/batch_proposal"]; exists {
		t.Fatal("failed HTTP request was correlated to a proposal")
	}
}

func TestLatencyDiagTagValidation(t *testing.T) {
	for _, tc := range []struct {
		value string
		want  uint64
	}{{"16", 16}, {"32", 32}, {"15", 0}, {"", 0}, {"secret", 0}, {"1016", 0}, {strings.Repeat("1", 21), 0}} {
		req := httptest.NewRequest(http.MethodGet, "/ready", nil)
		req.Header.Set(latencydiag.TagHeader, tc.value)
		if got := latencydiagTag(req); got != tc.want {
			t.Fatalf("tag %q got=%d want=%d", tc.value, got, tc.want)
		}
	}
}

func testTaggedHTTPTrace(t *testing.T) uint64 {
	wal, err := qlog.Open(t.TempDir() + "/qlog")
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()
	core := mustCore(t, "n1", []quepaxa.Member{{ID: "n1"}}, wal, nil)
	material, err := materializer.Open(t.TempDir()+"/db.sqlite", 1)
	if err != nil {
		t.Fatal(err)
	}
	defer material.Close()
	server := NewServer(core, material, "cluster", true, nil)
	defer server.Close()
	if _, err := server.Execute(context.Background(), ExecuteRequest{RequestID: "schema", SQL: "CREATE TABLE tagged_http (id INTEGER)"}); err != nil {
		t.Fatal(err)
	}
	latencydiag.Flush()
	body, _ := json.Marshal(ExecuteRequest{RequestID: "tagged-http", SQL: "INSERT INTO tagged_http VALUES (1)"})
	req := httptest.NewRequest(http.MethodPost, "/sql/execute", bytes.NewReader(body))
	req.Header.Set(latencydiag.TagHeader, strconv.FormatUint(16, 10))
	res := httptest.NewRecorder()
	server.ServeHTTP(res, req)
	if res.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", res.Code, res.Body.String())
	}
	var response ExecuteResponse
	if err := json.Unmarshal(res.Body.Bytes(), &response); err != nil || response.Slot == 0 {
		t.Fatalf("response slot=%d err=%v", response.Slot, err)
	}
	failed := httptest.NewRequest(http.MethodPost, "/sql/execute", bytes.NewBufferString("{"))
	failed.Header.Set(latencydiag.TagHeader, strconv.FormatUint(32, 10))
	failedResponse := httptest.NewRecorder()
	server.ServeHTTP(failedResponse, failed)
	if failedResponse.Code != http.StatusBadRequest {
		t.Fatalf("failed status=%d body=%s", failedResponse.Code, failedResponse.Body.String())
	}
	return response.Slot
}

//go:build rhiza_latency_diag

package recovery

import (
	"fmt"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/latencydiag"
	"github.com/thanos-io/objstore"
)

func TestArchiveBatchTraceRealRecursionAndFailure(t *testing.T) {
	latencydiag.DisableBackgroundWriterForTest()
	latencydiag.SetLosslessForTest(true)
	defer latencydiag.SetLosslessForTest(false)
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	old := os.Stderr
	os.Stderr = w
	defer func() { os.Stderr = old; _ = r.Close(); _ = w.Close() }()
	data := make(chan string, 1)
	go func() { b, _ := io.ReadAll(r); data <- string(b) }()
	if !t.Run("recursive", TestArchiveSyncThroughCoveredBatchRetriesLateHigherWaiter) {
		t.Fatal("recursive fixture failed")
	}
	if !t.Run("failure", TestArchiveSyncThroughCoveredSealedTargetIsError) {
		t.Fatal("failure fixture failed")
	}
	ctx, _, core, _, writer := newArchiveSyncAdmissionManagers(t)
	if _, _, err := core.Propose(ctx, []byte("same-slot requests")); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if err := writer.SyncThrough(ctx, core, 1); err != nil {
			t.Fatal(err)
		}
	}
	shutdown := NewManager(objstore.NewInMemBucket(), "", 1)
	shutdown.SetGroupDelay(time.Hour)
	shutdown.cancel()
	batch := &syncBatch{done: make(chan struct{})}
	batch.initTrace()
	shutdown.batch = batch
	shutdown.batchWG.Add(1)
	shutdown.flushBatch(batch, nil)
	shutdown.Close()
	latencydiag.Flush()
	_ = w.Close()
	log := <-data
	for _, want := range []string{"stage=archive_batch_wait", "stage=archive_batch_target", "stage=archive_batch_result", "stage=archive_slot_result", "stage=archive_head_upload", "stage=archive_extent_upload", "outcome=error"} {
		if !strings.Contains(log, want) {
			t.Errorf("missing trace %s", want)
		}
	}
	waits := map[string]bool{}
	results := map[string]bool{}
	uploads := map[string]bool{}
	covered, failed := false, false
	coveredSlots := map[string]bool{}
	secondSlotBatches := map[string]bool{}
	requestResults := map[string]int{}
	requestSlots := map[string]string{}
	requestBatches := map[string]map[string]bool{}
	slotRequests := map[string]map[string]bool{}
	attempts := map[string]map[string]bool{}
	shutdownResult := false
	for _, line := range strings.Split(log, "\n") {
		fields := map[string]string{}
		for _, field := range strings.Fields(line) {
			parts := strings.SplitN(field, "=", 2)
			if len(parts) == 2 {
				fields[parts[0]] = parts[1]
			}
		}
		switch fields["stage"] {
		case "archive_batch_wait":
			if fields["batch"] == "0" || fields["slot"] == "0" {
				t.Errorf("wait lacks exact identity: %s", line)
			}
			waits[fields["batch"]] = true
			if fields["request"] == "0" {
				t.Errorf("wait has no request identity: %s", line)
			}
			if requestBatches[fields["request"]] == nil {
				requestBatches[fields["request"]] = map[string]bool{}
			}
			requestBatches[fields["request"]][fields["batch"]] = true
			if fields["slot"] == "2" {
				secondSlotBatches[fields["batch"]] = true
			}
		case "archive_batch_result":
			results[fields["batch"]] = true
			if fields["batch"] == fmt.Sprint(batch.traceID()) && fields["outcome"] == "error" {
				shutdownResult = true
			}
		case "archive_slot_result":
			if fields["batch"] != "0" || fields["slot"] == "0" {
				t.Errorf("invalid final slot result: %s", line)
			}
			covered = covered || fields["outcome"] == "success_tip_covered"
			requestResults[fields["request"]]++
			requestSlots[fields["request"]] = fields["slot"]
			if slotRequests[fields["slot"]] == nil {
				slotRequests[fields["slot"]] = map[string]bool{}
			}
			slotRequests[fields["slot"]][fields["request"]] = true
			if fields["outcome"] == "success_tip_covered" {
				coveredSlots[fields["slot"]] = true
			}
			failed = failed || fields["outcome"] == "error"
		case "archive_head_upload", "archive_extent_upload":
			if fields["batch"] != "0" {
				uploads[fields["batch"]] = true
				if attempts[fields["batch"]] == nil {
					attempts[fields["batch"]] = map[string]bool{}
				}
				if fields["attempt"] == "0" || attempts[fields["batch"]][fields["attempt"]] {
					t.Errorf("duplicate or absent attempt: %s", line)
				}
				attempts[fields["batch"]][fields["attempt"]] = true
			}
		}
	}
	if len(waits) < 2 {
		t.Errorf("expected multiple distinct batch waits, got %d", len(waits))
	}
	if !covered || !failed {
		t.Errorf("missing final slot coverage or failure: covered=%v failed=%v", covered, failed)
	}
	if !coveredSlots["1"] || !coveredSlots["2"] || len(secondSlotBatches) < 2 {
		t.Errorf("recursive coverage not demonstrated: slots=%v slot2_batches=%d", coveredSlots, len(secondSlotBatches))
	}
	recursiveRequest := false
	for id, batches := range requestBatches {
		if len(batches) >= 2 && requestResults[id] == 1 && requestSlots[id] == "2" {
			recursiveRequest = true
		}
	}
	if !recursiveRequest {
		t.Error("no request ID spans recursive waits and exactly one slot-2 result")
	}
	if !shutdownResult {
		t.Error("shutdown batch has no error result")
	}
	if len(slotRequests["1"]) < 2 {
		t.Error("same-slot requests were not distinguished")
	}
	for id, count := range requestResults {
		if id == "0" || count != 1 {
			t.Errorf("request %s produced %d final results", id, count)
		}
	}
	multiUpload := false
	for _, ids := range attempts {
		if len(ids) >= 2 {
			multiUpload = true
		}
	}
	if !multiUpload {
		t.Error("no batch with distinct upload attempts")
	}
	for batch := range waits {
		if !results[batch] {
			t.Errorf("no result for waited batch %s", batch)
		}
	}
	if len(uploads) == 0 {
		t.Error("no upload carried a batch identity")
	}
	for batch := range uploads {
		if !results[batch] {
			t.Errorf("no result for upload batch %s", batch)
		}
	}
}

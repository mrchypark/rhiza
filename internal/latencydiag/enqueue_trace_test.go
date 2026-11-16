//go:build rhiza_latency_diag

package latencydiag

import (
	"bytes"
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"
)

func TestPreEnqueueTraceIsTaggedFixedAndBounded(t *testing.T) {
	flushMu.Lock()
	defer flushMu.Unlock()
	ring.drain()

	ctx := WithTag(context.Background(), 16)
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	started := StartTagged(ctx)
	TraceTaggedStage(cancelled, StagePreEnqueueValidate, errors.New("secret request text"), started)
	TraceRequestLockWait(ctx, 3548, nil, StartTagged(ctx))

	events, dropped, preEnqueueDropped := ring.drainWithStats()
	if dropped != 0 || preEnqueueDropped != 0 || len(events) != 2 {
		t.Fatalf("events=%+v dropped=%d pre_enqueue_dropped=%d", events, dropped, preEnqueueDropped)
	}
	if events[0].stage != StagePreEnqueueValidate || events[0].tag != 16 || events[0].outcome != "error" {
		t.Fatalf("validation event=%+v", events[0])
	}
	if events[1].stage != StageRequestLockWait || events[1].count != 3548 || events[1].outcome != "ok" {
		t.Fatalf("lock event=%+v", events[1])
	}
}

func TestPreEnqueueTraceSkipsUnsampledAndCancellationIsClassified(t *testing.T) {
	flushMu.Lock()
	defer flushMu.Unlock()
	ring.drain()

	unsampled := WithTag(context.Background(), 17)
	TraceTaggedStage(unsampled, StagePreEnqueueClone, nil, StartTagged(unsampled))
	ctx := WithTag(context.Background(), 32)
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	TraceTaggedStage(cancelled, StagePreEnqueueEncode, context.Canceled, StartTagged(ctx))

	events, _, _ := ring.drainWithStats()
	if len(events) != 1 || events[0].tag != 32 || events[0].outcome != "cancelled" {
		t.Fatalf("events=%+v", events)
	}
}

func TestProposalTraceCarriesEverySampledBatchTag(t *testing.T) {
	flushMu.Lock()
	defer flushMu.Unlock()
	ring.drain()

	ctx := WithTags(context.Background(), []uint64{15, 16, 32, 33})
	many := make([]uint64, maxTags*2)
	for i := range many {
		many[i] = uint64(i+1) * 16
	}
	if got := len(Tags(WithTags(context.Background(), many))); got != maxTags {
		t.Fatalf("bounded tags=%d want=%d", got, maxTags)
	}
	started := StartTagged(ctx)
	if started.IsZero() {
		t.Fatal("sampled batch did not start tagged trace")
	}
	TraceProposalStage(ctx, StageProposalPermitWait, ProposalWaitLocalCap|ProposalWaitOperationCap, nil, started)
	carried := CarryTags(context.Background(), ctx)
	TraceProposalStage(carried, StageProposalWorkerStart, 0, context.Canceled, started)

	events, drops := ring.drainWithDropStats()
	if drops.total != 0 || drops.proposalGap != 0 || len(events) != 4 {
		t.Fatalf("events=%+v drops=%+v", events, drops)
	}
	if events[0].tag != 16 || events[1].tag != 32 || events[0].count != 3 || events[0].outcome != "ok" {
		t.Fatalf("permit events=%+v", events[:2])
	}
	if events[2].tag != 16 || events[3].tag != 32 || events[2].stage != StageProposalWorkerStart || events[2].outcome != "cancelled" {
		t.Fatalf("worker events=%+v", events[2:])
	}
}

func TestProposalTraceDropAccountingIsSeparateAndBounded(t *testing.T) {
	var bounded eventRing
	for i := 0; i < capacity+3; i++ {
		bounded.add(event{stage: StageProposalMuWait, family: familyProposalGap, tag: uint64(i + 1), start: time.Now(), end: time.Now()})
	}
	events, drops := bounded.drainWithDropStats()
	if len(events) != capacity || drops.total != 3 || drops.proposalGap != 3 || drops.proposalGapCapacity != 3 ||
		drops.proposalGapTryLock != 0 || drops.proposalGapReasonIncomplete != 0 || drops.preEnqueue != 0 {
		t.Fatalf("events=%d drops=%+v", len(events), drops)
	}
}

func TestClassifiedDropIncompleteSurvivesZeroSnapshot(t *testing.T) {
	var counter atomic.Uint64
	var incomplete atomic.Bool
	incomplete.Store(true)
	_, _, reason := snapshotClassifiedDrop(&counter, &incomplete)
	if reason != 1 {
		t.Fatal("zero snapshot cleared incomplete state")
	}
	counter.Store(uint64(capacity) << 32)
	_, _, reason = snapshotClassifiedDrop(&counter, &incomplete)
	if reason != 1 {
		t.Fatal("following nonzero snapshot lost incomplete state")
	}
}

func TestProposalDropZeroSnapshotWriterBoundaryKeepsIncomplete(t *testing.T) {
	var bounded eventRing
	bounded.proposalGapDropIncomplete.Store(true)
	if _, drops := bounded.drainWithDropStats(); drops.proposalGap != 0 || drops.proposalGapReasonIncomplete != 1 {
		t.Fatalf("zero snapshot=%+v", drops)
	}

	start := make(chan struct{})
	result := make(chan uint64, 1)
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		<-start
		bounded.recordDrop(event{family: familyProposalGap}, dropReasonCapacity)
	}()
	go func() {
		defer wg.Done()
		<-start
		_, drops := bounded.drainWithDropStats()
		result <- drops.proposalGapCapacity
	}()
	close(start)
	wg.Wait()
	close(result)
	var seen uint64
	for capacity := range result {
		seen += capacity
	}
	_, drops := bounded.drainWithDropStats()
	if seen+drops.proposalGapCapacity != 1 || drops.proposalGapReasonIncomplete != 1 {
		t.Fatalf("raced seen=%d final=%+v", seen, drops)
	}
}

func TestFlushZeroDropIncompleteFailsTaggedAnalyzer(t *testing.T) {
	var stdout, stderr bytes.Buffer
	flushCmd := exec.Command(os.Args[0], "-test.run=^TestFlushZeroDropIncompleteChild$")
	flushCmd.Env = append(os.Environ(), "RHIZA_LATENCYDIAG_FLUSH_CHILD=1")
	flushCmd.Stdout = &stdout
	flushCmd.Stderr = &stderr
	if err := flushCmd.Run(); err != nil {
		t.Fatalf("flush child: %v stdout=%s stderr=%s", err, stdout.String(), stderr.String())
	}
	output := stderr.String()
	for _, line := range []string{
		"rhiza_latency pre_enqueue_dropped=0",
		"rhiza_latency pre_enqueue_dropped_reason_incomplete=1",
		"rhiza_latency proposal_gap_dropped=0",
		"rhiza_latency proposal_gap_dropped_capacity=0",
		"rhiza_latency proposal_gap_dropped_trylock=0",
		"rhiza_latency proposal_gap_dropped_reason_incomplete=1",
	} {
		if !strings.Contains(output, line+"\n") {
			t.Fatalf("flush output missing %q: %q", line, output)
		}
	}

	clientFile := filepath.Join(t.TempDir(), "client.tsv")
	if err := os.WriteFile(clientFile, []byte("16\t0\t1000\t9000\t1100\t8500\t7000\ttrue\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	_, source, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate latencydiag test source")
	}
	analyzer := filepath.Join(filepath.Dir(source), "..", "..", "scripts", "ci", "assess-tagged-latency.awk")
	cmd := exec.Command("awk", "-v", "client_file="+clientFile, "-f", analyzer)
	cmd.Stdin = strings.NewReader(output)
	assessment, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("analyzer accepted zero-drop incomplete marker: %s", assessment)
	}
	if !strings.Contains(string(assessment), "# correlation_status=invalid\n") ||
		!strings.Contains(string(assessment), "# proposal_gap_dropped_reason_incomplete=1\n") {
		t.Fatalf("analyzer did not fail closed on flush output: %s", assessment)
	}

	preAnalyzer := filepath.Join(filepath.Dir(source), "..", "..", "scripts", "ci", "assess-preenqueue-latency.awk")
	preCmd := exec.Command("awk", "-v", "client_file="+clientFile, "-f", preAnalyzer)
	preCmd.Stdin = strings.NewReader(output)
	preAssessment, err := preCmd.CombinedOutput()
	if err == nil {
		t.Fatalf("pre-enqueue analyzer accepted zero-drop incomplete marker: %s", preAssessment)
	}
	if !strings.Contains(string(preAssessment), "# status=invalid\n") ||
		!strings.Contains(string(preAssessment), "# pre_enqueue_dropped_reason_incomplete=1\n") {
		t.Fatalf("pre-enqueue analyzer did not fail closed on flush output: %s", preAssessment)
	}
}

func TestFlushZeroDropIncompleteChild(t *testing.T) {
	if os.Getenv("RHIZA_LATENCYDIAG_FLUSH_CHILD") != "1" {
		t.Skip("flush child")
	}
	DisableBackgroundWriterForTest()
	ring.preEnqueueDropIncomplete.Store(true)
	ring.proposalGapDropIncomplete.Store(true)
	Flush()
}

func TestPreEnqueueRingOverflowCountsOnlyDroppedFamily(t *testing.T) {
	var bounded eventRing
	for i := 0; i < capacity; i++ {
		bounded.add(event{stage: StagePreEnqueueClone, family: familyPreEnqueue, tag: uint64(i), start: time.Now(), end: time.Now()})
	}
	bounded.add(event{stage: "other", family: familyOther, start: time.Now(), end: time.Now()})
	bounded.add(event{stage: StageBatcherEnqueue, family: familyPreEnqueue, tag: 99, start: time.Now(), end: time.Now()})
	events, drops := bounded.drainWithDropStats()
	if len(events) != capacity || drops.total != 2 || drops.preEnqueue != 2 ||
		drops.preEnqueueCapacity != 2 || drops.preEnqueueTryLock != 0 {
		t.Fatalf("events=%d drops=%+v", len(events), drops)
	}
	if events[len(events)-1].stage != StageBatcherEnqueue || events[len(events)-1].tag != 99 {
		t.Fatalf("newest event not retained: %+v", events[len(events)-1])
	}
}

func TestPreEnqueueRingDistinguishesCapacityFromTryLockDrops(t *testing.T) {
	var bounded eventRing
	bounded.mu.Lock()
	recorded := make(chan struct{})
	go func() {
		bounded.add(event{stage: StageRequestLockWait, family: familyPreEnqueue, tag: 16, start: time.Now(), end: time.Now()})
		close(recorded)
	}()
	select {
	case <-recorded:
	case <-time.After(time.Second):
		bounded.mu.Unlock()
		t.Fatal("TryLock drop path waited for the contended ring")
	}
	bounded.mu.Unlock()
	events, drops := bounded.drainWithDropStats()
	if len(events) != 0 || drops.total != 1 || drops.preEnqueue != 1 ||
		drops.preEnqueueCapacity != 0 || drops.preEnqueueTryLock != 1 {
		t.Fatalf("events=%d trylock drops=%+v", len(events), drops)
	}
}

func TestPreEnqueueDropReasonsRemainExactUnderConcurrentAccounting(t *testing.T) {
	var bounded eventRing
	const each = 1000
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < each; i++ {
			bounded.recordDrop(event{family: familyPreEnqueue}, dropReasonCapacity)
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < each; i++ {
			bounded.recordDrop(event{family: familyPreEnqueue}, dropReasonTryLock)
		}
	}()
	wg.Wait()
	_, drops := bounded.drainWithDropStats()
	if drops.total != each*2 || drops.preEnqueue != each*2 ||
		drops.preEnqueueCapacity != each || drops.preEnqueueTryLock != each ||
		drops.preEnqueueReasonIncomplete != 0 {
		t.Fatalf("concurrent drops=%+v", drops)
	}
}

func TestPreEnqueueCapacityReasonSaturatesAndMarksIncomplete(t *testing.T) {
	var bounded eventRing
	maxLane := uint32(^uint32(0))
	bounded.preEnqueueDropState.Store(uint64(maxLane) << 32)

	bounded.recordDrop(event{family: familyPreEnqueue}, dropReasonCapacity)
	_, drops := bounded.drainWithDropStats()
	if drops.total != 1 || drops.preEnqueue != uint64(maxLane) ||
		drops.preEnqueueCapacity != uint64(maxLane) || drops.preEnqueueTryLock != 0 ||
		drops.preEnqueueReasonIncomplete != 1 {
		t.Fatalf("saturated capacity drops=%+v", drops)
	}
}

func TestPreEnqueueDropAccountingSurvivesConcurrentDrain(t *testing.T) {
	var bounded eventRing
	const each = 1000
	paused := make(chan struct{})
	firstDrain := make(chan struct{})
	resume := make(chan struct{})
	writerDone := make(chan struct{})

	go func() {
		for i := 0; i < each; i++ {
			bounded.recordDrop(event{family: familyPreEnqueue}, dropReasonCapacity)
		}
		close(paused)
		<-resume
		for i := 0; i < each; i++ {
			bounded.recordDrop(event{family: familyPreEnqueue}, dropReasonTryLock)
		}
		close(writerDone)
	}()

	drained := make(chan dropStats, 1)
	go func() {
		var total dropStats
		signaled := false
		for {
			_, drops := bounded.drainWithDropStats()
			total.total += drops.total
			total.preEnqueue += drops.preEnqueue
			total.preEnqueueCapacity += drops.preEnqueueCapacity
			total.preEnqueueTryLock += drops.preEnqueueTryLock
			total.preEnqueueReasonIncomplete += drops.preEnqueueReasonIncomplete
			if !signaled {
				close(firstDrain)
				signaled = true
			}
			select {
			case <-writerDone:
				_, final := bounded.drainWithDropStats()
				total.total += final.total
				total.preEnqueue += final.preEnqueue
				total.preEnqueueCapacity += final.preEnqueueCapacity
				total.preEnqueueTryLock += final.preEnqueueTryLock
				total.preEnqueueReasonIncomplete += final.preEnqueueReasonIncomplete
				drained <- total
				return
			default:
				runtime.Gosched()
			}
		}
	}()

	<-paused
	<-firstDrain
	close(resume)
	<-writerDone
	drops := <-drained
	if drops.total != each*2 || drops.preEnqueue != each*2 ||
		drops.preEnqueueCapacity != each || drops.preEnqueueTryLock != each ||
		drops.preEnqueueReasonIncomplete != 0 {
		t.Fatalf("writer/drain drops=%+v", drops)
	}
}

func TestDiagnosticRingCapacityContract(t *testing.T) {
	const (
		expectedCapacity       = 384
		previousCapacity       = 256
		ringStorageBudgetBytes = 64 << 10
		peakFlushBudgetBytes   = 112 << 10
	)
	eventBytes := unsafe.Sizeof(event{})
	ringBytes := eventBytes * uintptr(capacity)
	previousRingBytes := eventBytes * uintptr(previousCapacity)
	maxFlushCopyBytes := ringBytes
	previousMaxFlushCopyBytes := previousRingBytes
	steadyDelta := int64(ringBytes - previousRingBytes)
	transientDelta := int64(maxFlushCopyBytes - previousMaxFlushCopyBytes)
	peakFlushDelta := steadyDelta + transientDelta
	t.Logf("capacity=%d event_bytes=%d ring_bytes=%d previous_ring_bytes=%d steady_delta_bytes=%d max_flush_copy_bytes=%d previous_max_flush_copy_bytes=%d transient_alloc_delta_bytes=%d peak_flush_memory_delta_bytes=%d",
		capacity, eventBytes, ringBytes, previousRingBytes, steadyDelta,
		maxFlushCopyBytes, previousMaxFlushCopyBytes, transientDelta, peakFlushDelta)
	if capacity != expectedCapacity ||
		ringBytes > ringStorageBudgetBytes ||
		maxFlushCopyBytes > ringStorageBudgetBytes ||
		ringBytes+maxFlushCopyBytes > peakFlushBudgetBytes {
		t.Fatalf("capacity=%d event_bytes=%d ring_bytes=%d max_flush_copy_bytes=%d", capacity, eventBytes, ringBytes, maxFlushCopyBytes)
	}
}

func TestPreEnqueueRingHighLoadKeepsBoundedFIFOAndAccounting(t *testing.T) {
	var bounded eventRing
	const attempts = capacity*4 + 17
	for i := 0; i < attempts; i++ {
		bounded.add(event{stage: StagePreEnqueueClone, family: familyPreEnqueue, tag: uint64(i + 1), start: time.Now(), end: time.Now()})
	}
	events, drops := bounded.drainWithDropStats()
	const dropped = attempts - capacity
	if len(events) != capacity || drops.total != dropped || drops.preEnqueue != dropped ||
		drops.preEnqueueCapacity != dropped || drops.preEnqueueTryLock != 0 ||
		drops.preEnqueueReasonIncomplete != 0 {
		t.Fatalf("events=%d drops=%+v", len(events), drops)
	}
	if events[0].tag != dropped+1 || events[len(events)-1].tag != attempts {
		t.Fatalf("FIFO bounds first=%d last=%d", events[0].tag, events[len(events)-1].tag)
	}
}

func TestPreEnqueueRingConcurrentHighLoadAccounting(t *testing.T) {
	var bounded eventRing
	const writers = 8
	const attempts = writers * capacity
	var wg sync.WaitGroup
	wg.Add(writers)
	for writer := 0; writer < writers; writer++ {
		go func() {
			defer wg.Done()
			for i := 0; i < capacity; i++ {
				bounded.add(event{stage: StageRequestLockHold, family: familyPreEnqueue, tag: uint64(i + 1), start: time.Now(), end: time.Now()})
			}
		}()
	}
	wg.Wait()
	events, drops := bounded.drainWithDropStats()
	if len(events)+int(drops.total) != attempts ||
		drops.preEnqueue != drops.total ||
		drops.preEnqueue != drops.preEnqueueCapacity+drops.preEnqueueTryLock ||
		drops.preEnqueueReasonIncomplete != 0 {
		t.Fatalf("events=%d drops=%+v", len(events), drops)
	}
}

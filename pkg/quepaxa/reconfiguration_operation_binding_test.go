package quepaxa

import (
	"bytes"
	"context"
	"crypto/sha256"
	"strings"
	"testing"
	"time"
)

func TestReconfigurationControlRetainsOperationDigest(t *testing.T) {
	base := reconfigurationValue{Freeze: 1, TerminalSlot: 17, Target: Cluster{ConfigID: 2, Members: []Member{{ID: "a"}, {ID: "b"}}}}
	controlAValue := base
	controlAValue.OperationDigest[0] = 0x11
	controlBValue := base
	controlBValue.OperationDigest[0] = 0x22
	controlA, err := encodeReconfiguration(controlAValue)
	if err != nil {
		t.Fatal(err)
	}
	controlB, err := encodeReconfiguration(controlBValue)
	if err != nil {
		t.Fatal(err)
	}

	decodedA, ok, err := decodeReconfiguration(controlA)
	if err != nil || !ok {
		t.Fatalf("decode bound control A: ok=%v err=%v", ok, err)
	}
	decodedB, ok, err := decodeReconfiguration(controlB)
	if err != nil || !ok {
		t.Fatalf("decode bound control B: ok=%v err=%v", ok, err)
	}
	if decodedA.OperationDigest == decodedB.OperationDigest {
		t.Fatal("distinct operation digests were not retained")
	}
	if reconfigurationHash(controlA) == reconfigurationHash(controlB) {
		t.Fatal("different operation digests produced the same certified control hash")
	}

	legacy, err := encodeReconfiguration(base)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(legacy, []byte("operation_digest")) {
		t.Fatalf("legacy control encoding changed: %s", legacy)
	}
	if _, ok, err := decodeReconfiguration(legacy); err != nil || !ok {
		t.Fatalf("legacy unbound control no longer decodes: ok=%v err=%v", ok, err)
	}
}

func TestBoundReconfigurationRequiresExactOperationDigest(t *testing.T) {
	cores, _ := reconfigCluster(t)
	core := cores["a"]
	ctx := context.Background()
	target := Cluster{ConfigID: 2, Members: []Member{{ID: "a"}, {ID: "b"}}}
	var digest, other [32]byte
	digest[0] = 0x31
	other[0] = 0x32

	if _, err := core.BeginReconfigurationBoundAt(ctx, target, 0, digest); err != nil {
		t.Fatal(err)
	}
	core.mu.RLock()
	freezeValue, ok := core.decided[1]
	core.mu.RUnlock()
	if !ok {
		t.Fatal("bound freeze was not retained")
	}
	freeze, ok, err := decodeReconfiguration(freezeValue.Value)
	if err != nil || !ok || freeze.OperationDigest != digest {
		t.Fatalf("freeze digest=%q ok=%v err=%v", freeze.OperationDigest, ok, err)
	}

	if err := core.FinishReconfiguration(ctx); err == nil {
		t.Fatal("legacy finish completed a bound frozen operation")
	}
	if err := core.FinishReconfigurationBoundAt(ctx, target, 0, other); err == nil {
		t.Fatal("finish accepted a different operation digest")
	}
	if _, pending := core.PendingReconfiguration(); !pending {
		t.Fatal("rejected finish cleared the frozen operation")
	}
	if err := core.FinishReconfigurationBoundAt(ctx, target, 0, digest); err != nil {
		t.Fatal(err)
	}

	history, err := core.MembershipHistory()
	if err != nil {
		t.Fatal(err)
	}
	if len(history.Transitions) != 1 {
		t.Fatalf("history transitions=%d want 1", len(history.Transitions))
	}
	terminal, ok, err := decodeReconfiguration(history.Transitions[0].Terminal.Value)
	if err != nil || !ok || terminal.OperationDigest != freeze.OperationDigest {
		t.Fatalf("terminal digest=%q freeze digest=%q ok=%v err=%v", terminal.OperationDigest, freeze.OperationDigest, ok, err)
	}
	if _, err := validateMembershipHistory(history.Genesis, history); err != nil {
		t.Fatalf("bound history did not replay: %v", err)
	}
	if _, _, err := core.Propose(ctx, []byte("after bound reconfiguration")); err != nil {
		t.Fatal(err)
	}
	checkpoint, err := core.CheckpointMembership(core.Tip())
	if err != nil {
		t.Fatal(err)
	}
	if len(checkpoint.Transitions) != 1 {
		t.Fatalf("checkpoint transitions=%d want 1", len(checkpoint.Transitions))
	}
	checkpointFreeze, _, err := decodeReconfiguration(checkpoint.Transitions[0].Freeze.Value)
	checkpointTerminal, _, terminalErr := decodeReconfiguration(checkpoint.Transitions[0].Terminal.Value)
	if err != nil || terminalErr != nil || checkpointFreeze.OperationDigest != digest || checkpointTerminal.OperationDigest != digest {
		t.Fatalf("checkpoint did not preserve operation digest: freeze=%q terminal=%q errors=%v/%v", checkpointFreeze.OperationDigest, checkpointTerminal.OperationDigest, err, terminalErr)
	}
	prefix, ok := core.PrefixHash(core.Tip())
	if !ok {
		t.Fatal("checkpoint prefix is unavailable")
	}
	next, following, err := core.CheckpointLeaderOrders(core.Tip())
	if err != nil {
		t.Fatal(err)
	}
	seal := CheckpointSeal{
		ConfigID: core.ConfigID(), Index: core.Tip(), RootHash: sha256.Sum256([]byte("root")),
		StateHash: sha256.Sum256([]byte("state")), PrefixHash: prefix,
		NextLeaderOrder: next, FollowingLeaderOrder: following, Membership: &checkpoint,
	}
	encodedSeal, err := EncodeCheckpointSeal(seal)
	if err != nil {
		t.Fatal(err)
	}
	decodedSeal, isSeal, err := DecodeCheckpointSeal(encodedSeal)
	if err != nil || !isSeal {
		t.Fatalf("checkpoint seal decode: isSeal=%v err=%v", isSeal, err)
	}
	if _, err := core.checkpointMembershipForSeal(decodedSeal); err != nil {
		t.Fatalf("checkpoint round-trip lost operation binding: %v", err)
	}
}

func TestBoundReconfigurationAbortRetainsOperationDigest(t *testing.T) {
	cores, _ := reconfigCluster(t)
	core := cores["a"]
	ctx := context.Background()
	target := Cluster{ConfigID: 2, Members: []Member{{ID: "a"}, {ID: "b"}, {ID: "c"}, {ID: "candidate", WALIdentity: strings.Repeat("2", 64)}}}
	var digest, wrong [32]byte
	digest[0] = 0x41
	wrong[0] = 0x42
	if _, err := core.BeginReconfigurationBoundAt(ctx, target, 0, digest); err != nil {
		t.Fatal(err)
	}
	if err := core.AbortReconfigurationAt(ctx, target, 0); err == nil {
		t.Fatal("legacy abort completed a bound frozen operation")
	}
	if err := core.AbortReconfigurationBoundAt(ctx, target, 0, wrong); err == nil {
		t.Fatal("abort accepted a different operation digest")
	}
	if err := core.AbortReconfigurationBoundAt(ctx, target, 0, digest); err != nil {
		t.Fatal(err)
	}
	history, err := core.MembershipHistory()
	if err != nil {
		t.Fatal(err)
	}
	if history.Abort == nil {
		t.Fatal("bound abort missing from membership history")
	}
	freeze, freezeOK, freezeErr := decodeReconfiguration(history.Abort.Freeze.Value)
	terminal, terminalOK, terminalErr := decodeReconfiguration(history.Abort.Terminal.Value)
	if freezeErr != nil || terminalErr != nil || !freezeOK || !terminalOK || freeze.OperationDigest != digest || terminal.OperationDigest != digest {
		t.Fatalf("abort digest mismatch: freeze=%x terminal=%x errors=%v/%v", freeze.OperationDigest, terminal.OperationDigest, freezeErr, terminalErr)
	}
	if _, err := validateMembershipHistory(history.Genesis, history); err != nil {
		t.Fatalf("bound abort history did not replay: %v", err)
	}
}

func TestBoundReconfigurationLostResponseRetry(t *testing.T) {
	cores, transport := reconfigCluster(t)
	core := cores["a"]
	ctx := context.Background()
	target := Cluster{ConfigID: 2, Members: []Member{{ID: "a"}, {ID: "b"}}}
	var digest [32]byte
	digest[0] = 0x61
	if _, err := core.BeginReconfigurationBoundAt(ctx, target, 0, digest); err != nil {
		t.Fatal(err)
	}
	if err := core.FinishReconfigurationBoundAt(ctx, target, 0, digest); err != nil {
		t.Fatal(err)
	}
	if err := core.FinishReconfigurationBoundAt(ctx, target, 0, digest); err != nil {
		t.Fatalf("exact lost-response retry failed: %v", err)
	}
	secondTarget := Cluster{ConfigID: 3, Members: []Member{{ID: "b"}}}
	var secondDigest [32]byte
	secondDigest[0] = 0x66
	if _, err := core.BeginReconfigurationBoundAt(ctx, secondTarget, 0, secondDigest); err != nil {
		t.Fatalf("begin following operation: %v", err)
	}
	core.mu.RLock()
	secondFreeze := core.reconfiguration.freeze
	core.mu.RUnlock()
	transport.mu.Lock()
	transport.down["b"], transport.down["c"] = true, true
	transport.mu.Unlock()
	if err := core.FinishReconfigurationBoundAt(ctx, target, 0, digest); err == nil {
		t.Fatal("historical completion succeeded without the required old-voter ACK quorum")
	}
	core.mu.RLock()
	stillSecond := core.reconfiguration != nil && core.reconfiguration.freeze == secondFreeze
	core.mu.RUnlock()
	if !stillSecond {
		t.Fatal("historical retry changed or cleared the active later freeze")
	}
	transport.mu.Lock()
	delete(transport.down, "b")
	delete(transport.down, "c")
	transport.mu.Unlock()
	if err := core.FinishReconfigurationBoundAt(ctx, target, 0, digest); err != nil {
		t.Fatalf("retry of prior operation while another freeze is active failed: %v", err)
	}
	core.mu.RLock()
	stillSecond = core.reconfiguration != nil && core.reconfiguration.freeze == secondFreeze
	core.mu.RUnlock()
	if !stillSecond {
		t.Fatal("historical retry changed or cleared the active later freeze")
	}
	if err := core.FinishReconfigurationBoundAt(ctx, secondTarget, 0, secondDigest); err != nil {
		t.Fatalf("finish following operation: %v", err)
	}
	if err := core.FinishReconfigurationBoundAt(ctx, target, 0, digest); err != nil {
		t.Fatalf("retry of first operation after later terminal failed: %v", err)
	}
	wrongTarget := Cluster{ConfigID: 2, Members: []Member{{ID: "a"}, {ID: "c"}}}
	var wrongDigest [32]byte
	wrongDigest[0] = 0x62
	if err := core.FinishReconfigurationBoundAt(ctx, wrongTarget, 0, digest); err == nil {
		t.Fatal("retry accepted a different target")
	}
	if err := core.FinishReconfigurationBoundAt(ctx, target, 1, digest); err == nil {
		t.Fatal("retry accepted a different prior abort revision")
	}
	if err := core.FinishReconfigurationBoundAt(ctx, target, 0, wrongDigest); err == nil {
		t.Fatal("retry accepted a different operation digest")
	}
	if err := core.AbortReconfigurationBoundAt(ctx, target, 0, digest); err == nil {
		t.Fatal("finish history was accepted as an abort")
	}
}

func TestBoundReconfigurationRetriesPriorTerminalWithoutChangingActiveOrLaterRound(t *testing.T) {
	cores, _ := reconfigCluster(t)
	core := cores["a"]
	ctx := context.Background()
	firstTarget := Cluster{ConfigID: 2, Members: []Member{{ID: "a"}, {ID: "b"}}}
	secondTarget := Cluster{ConfigID: 3, Members: []Member{{ID: "b"}}}
	var firstDigest, secondDigest [32]byte
	firstDigest[0], secondDigest[0] = 0x71, 0x72
	if _, err := core.BeginReconfigurationBoundAt(ctx, firstTarget, 0, firstDigest); err != nil {
		t.Fatal(err)
	}
	if err := core.FinishReconfigurationBoundAt(ctx, firstTarget, 0, firstDigest); err != nil {
		t.Fatal(err)
	}
	if _, err := core.BeginReconfigurationBoundAt(ctx, secondTarget, 0, secondDigest); err != nil {
		t.Fatal(err)
	}
	core.mu.RLock()
	secondFreeze := core.reconfiguration.freeze
	core.mu.RUnlock()
	if err := core.FinishReconfigurationBoundAt(ctx, firstTarget, 0, firstDigest); err != nil {
		t.Fatalf("prior terminal retry while later round is active: %v", err)
	}
	core.mu.RLock()
	active := core.reconfiguration
	core.mu.RUnlock()
	if active == nil || active.freeze != secondFreeze || !sameCluster(active.target, secondTarget) {
		t.Fatal("prior terminal retry modified the active later round")
	}
	if err := core.FinishReconfigurationBoundAt(ctx, secondTarget, 0, secondDigest); err != nil {
		t.Fatal(err)
	}
	if err := core.FinishReconfigurationBoundAt(ctx, firstTarget, 0, firstDigest); err != nil {
		t.Fatalf("prior terminal retry after later terminal: %v", err)
	}
}

func TestBoundReconfigurationAbortLostResponseRetry(t *testing.T) {
	cores, _ := reconfigCluster(t)
	core := cores["a"]
	ctx := context.Background()
	target := Cluster{ConfigID: 2, Members: []Member{{ID: "a"}, {ID: "b"}, {ID: "c"}, {ID: "candidate", WALIdentity: strings.Repeat("2", 64)}}}
	var digest [32]byte
	digest[0] = 0x63
	if _, err := core.BeginReconfigurationBoundAt(ctx, target, 0, digest); err != nil {
		t.Fatal(err)
	}
	if err := core.AbortReconfigurationBoundAt(ctx, target, 0, digest); err != nil {
		t.Fatal(err)
	}
	if err := core.AbortReconfigurationBoundAt(ctx, target, 0, digest); err != nil {
		t.Fatalf("exact abort lost-response retry failed: %v", err)
	}
	if err := core.FinishReconfigurationBoundAt(ctx, target, 0, digest); err == nil {
		t.Fatal("abort history was accepted as a finish")
	}
	if err := core.AbortReconfigurationBoundAt(ctx, target, 1, digest); err == nil {
		t.Fatal("abort retry accepted a different prior abort revision")
	}
	core.mu.Lock()
	core.floor = core.lastAbort.Terminal.Slot
	core.mu.Unlock()
	if err := core.AbortReconfigurationBoundAt(ctx, target, 0, digest); err == nil {
		t.Fatal("compacted history was accepted as retry proof")
	}
}

func TestBoundReconfigurationRetryAfterPriorAbortUsesExactRevision(t *testing.T) {
	cores, _ := reconfigCluster(t)
	core := cores["a"]
	ctx := context.Background()
	target := Cluster{ConfigID: 2, Members: []Member{{ID: "a"}, {ID: "b"}}}
	abortTarget := Cluster{ConfigID: 2, Members: []Member{{ID: "a"}, {ID: "b"}, {ID: "c"}, {ID: "candidate", WALIdentity: strings.Repeat("3", 64)}}}
	var firstDigest, secondDigest [32]byte
	firstDigest[0], secondDigest[0] = 0x64, 0x65
	if _, err := core.BeginReconfigurationBoundAt(ctx, abortTarget, 0, firstDigest); err != nil {
		t.Fatal(err)
	}
	if err := core.AbortReconfigurationBoundAt(ctx, abortTarget, 0, firstDigest); err != nil {
		t.Fatal(err)
	}
	core.mu.RLock()
	priorAbort := core.lastAbort.Terminal.Slot
	core.mu.RUnlock()
	if _, err := core.BeginReconfigurationBoundAt(ctx, target, priorAbort, secondDigest); err != nil {
		t.Fatal(err)
	}
	if err := core.FinishReconfigurationBoundAt(ctx, target, priorAbort, secondDigest); err != nil {
		t.Fatal(err)
	}
	if err := core.FinishReconfigurationBoundAt(ctx, target, priorAbort, secondDigest); err != nil {
		t.Fatalf("retry with retained prior abort revision failed: %v", err)
	}
	if err := core.FinishReconfigurationBoundAt(ctx, target, 0, secondDigest); err == nil {
		t.Fatal("retry accepted the wrong prior abort revision")
	}
}

func TestMismatchedBoundTerminalIsRejectedBeforeDecisionRetention(t *testing.T) {
	cores, _ := reconfigCluster(t)
	core := cores["a"]
	target := Cluster{ConfigID: 2, Members: []Member{{ID: "a"}, {ID: "b"}}}
	var digest, wrong [32]byte
	digest[0] = 0x51
	wrong[0] = 0x52
	if _, err := core.BeginReconfigurationBoundAt(context.Background(), target, 0, digest); err != nil {
		t.Fatal(err)
	}
	core.mu.RLock()
	freeze, ok := core.decided[1]
	core.mu.RUnlock()
	if !ok {
		t.Fatal("bound freeze is missing")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := core.RecoverThrough(ctx, 16); err != nil {
		t.Fatal(err)
	}
	drainPrefix, ok := core.PrefixHash(16)
	if !ok {
		t.Fatal("bound drain prefix is missing")
	}
	terminalValue, err := encodeReconfiguration(reconfigurationValue{
		Terminal: true, Abort: true, Freeze: 1, TerminalSlot: 17, Target: target, PrefixHash: drainPrefix, OperationDigest: wrong,
	})
	if err != nil {
		t.Fatal(err)
	}
	step := Step(4)
	decision := Decision{
		ConfigID: 1, Slot: 17, Step: step,
		Proposal: Proposal{Hash: reconfigurationHash(terminalValue), Value: terminalValue},
		Summaries: []Summary{
			{RecorderID: "a", ReconfigurationID: freeze.Hash, Step: step},
			{RecorderID: "b", ReconfigurationID: freeze.Hash, Step: step},
		},
	}
	request := RecordRequest{Slot: 17, Step: step, ConfigID: 1, ReconfigurationID: freeze.Hash, Proposal: decision.Proposal}
	if _, err := core.Record(ctx, request); err == nil || !strings.Contains(err.Error(), "operation digest") {
		t.Fatalf("mismatched terminal record error=%v, want operation-digest rejection", err)
	}
	err = core.acceptDecision(decision)
	if err == nil || !strings.Contains(err.Error(), "operation digest") {
		t.Fatalf("mismatched terminal error=%v, want operation-digest rejection", err)
	}
	core.mu.RLock()
	_, retained := core.decided[17]
	core.mu.RUnlock()
	if retained {
		t.Fatal("mismatched terminal was retained before rejection")
	}
}

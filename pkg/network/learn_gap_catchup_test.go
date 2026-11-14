//go:build rhiza_local_testhooks

package network

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/localtesthooks"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/network/peerfb"
	"github.com/mrchypark/rhiza/pkg/qlog"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

// A single surviving remote voter can durably accept a later decision while
// its Learned acknowledgement waits for an earlier certified decision.
func TestLearnedGapWithOneRemoteSurvivorWaitsForCertifiedPrefix(t *testing.T) {
	cluster := newForegroundAPIPeers(t, t.TempDir())
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	encode := func(id string) []byte {
		t.Helper()
		item, err := types.EncodeKVBatchItem(types.KVCommand{RequestID: id, Operation: "put", Key: id, Value: []byte(id), ObservedAtUnixMS: time.Now().UnixMilli()})
		if err != nil {
			t.Fatal(err)
		}
		return types.AssembleKVBatch([][]byte{item})
	}
	received := make(chan struct{}, 1)
	release := make(chan struct{})
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(release) })
	restore := localtesthooks.Set(func(event string) {
		if strings.Contains(event, "network:learned:phase=received:node=n2:") && strings.Contains(event, ":slot=1") {
			select {
			case received <- struct{}{}:
			default:
			}
			<-release
			runtime.Goexit()
		}
	})
	defer restore()
	first := make(chan error, 1)
	go func() { _, err := cluster.servers["n1"].proposeOnce(ctx, encode("survivor-gap")); first <- err }()
	select {
	case <-received:
	case <-ctx.Done():
		t.Fatal("missing blocked n2 Learned(1)")
	}
	select {
	case err := <-first:
		if err != nil {
			t.Fatalf("first quorum: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("first quorum did not finish")
	}
	if !cluster.cores["n1"].IsDecided(1) || !cluster.cores["n3"].IsDecided(1) || cluster.cores["n2"].IsDecided(1) {
		t.Fatal("first certified decision or gated survivor state missing")
	}
	releaseOnce.Do(func() { close(release) })
	restore()
	if err := cluster.peerServers[2].Close(); err != nil {
		t.Fatalf("stop n3 peer: %v", err)
	}
	catchupStarted := make(chan struct{}, 32)
	allowCatchup := make(chan struct{})
	var allowOnce sync.Once
	defer allowOnce.Do(func() { close(allowCatchup) })
	var noGapFetch atomic.Bool
	response := make(chan struct{}, 1)
	restoreHigher := localtesthooks.Set(func(event string) {
		if !strings.Contains(event, "node=n2:") {
			return
		}
		if strings.Contains(event, "phase=prefix-catchup-start") && strings.Contains(event, ":slot=3") {
			noGapFetch.Store(true)
		}
		if !strings.Contains(event, ":slot=2") {
			return
		}
		if strings.Contains(event, "phase=prefix-catchup-start") {
			select {
			case catchupStarted <- struct{}{}:
			default:
			}
			<-allowCatchup
		}
		if strings.Contains(event, "phase=response-written") {
			select {
			case response <- struct{}{}:
			default:
			}
		}
	})
	defer restoreHigher()
	second := make(chan error, 1)
	go func() { _, err := cluster.servers["n1"].proposeOnce(ctx, encode("survivor-next")); second <- err }()
	select {
	case <-catchupStarted:
	case err := <-second:
		t.Fatalf("second returned before survivor apply: %v", err)
	case <-ctx.Done():
		t.Fatal("survivor did not receive later decision")
	}
	if !cluster.cores["n2"].IsDecided(2) || cluster.cores["n2"].IsDecided(1) || cluster.cores["n2"].Tip() != 0 {
		t.Fatal("survivor did not retain certified later decision behind missing prefix")
	}
	select {
	case <-response:
		t.Fatal("survivor ACK before prefix was installed")
	case err := <-second:
		t.Fatalf("proposal returned before prefix repair: %v", err)
	default:
	}
	// Re-deliver the exact certified decision while the first handler is held.
	// Bad certificates and configurations must fail before entering catch-up.
	values, _, err := cluster.cores["n1"].DecisionsFrom(2, 1)
	if err != nil || len(values) != 1 || values[0].Slot != 2 {
		t.Fatalf("load certified second decision: values=%v err=%v", values, err)
	}
	var decision quepaxa.Decision
	if err := json.Unmarshal(values[0].Certificate, &decision); err != nil {
		t.Fatal(err)
	}
	decision.Proposal.Value = values[0].Value
	probe := cluster.transports[0]
	request := func(d quepaxa.Decision) *peerfb.RequestT {
		r := probe.request(peerfb.OperationLearned)
		r.Decision = decisionToWire(d)
		return r
	}
	badCert := decision
	badCert.Summaries = nil
	if _, err := probe.callQuorum(ctx, "n2", request(badCert)); err == nil {
		t.Fatal("invalid certificate was accepted")
	}
	badConfig := request(decision)
	badConfig.ConfigId++
	if _, err := probe.callQuorum(ctx, "n2", badConfig); err == nil {
		t.Fatal("invalid configuration was accepted")
	}
	const concurrentDuplicates = 16
	duplicate := make(chan error, concurrentDuplicates)
	for i := 0; i < concurrentDuplicates; i++ {
		go func() { _, err := probe.callQuorum(ctx, "n2", request(decision)); duplicate <- err }()
	}
	for i := 0; i < concurrentDuplicates; i++ {
		select {
		case <-catchupStarted:
		case <-ctx.Done():
			t.Fatalf("admitted duplicate %d/%d", i, concurrentDuplicates)
		}
	}
	if n := len(cluster.peerServers[1].streams); n < concurrentDuplicates || n > maxPeerStreams {
		t.Fatalf("active peer admission=%d", n)
	}
	cancelCtx, stopCall := context.WithTimeout(ctx, 100*time.Millisecond)
	defer stopCall()
	if _, err := probe.callQuorum(cancelCtx, "n2", request(decision)); err == nil {
		t.Fatal("cancelled delivery returned an ACK")
	}
	select {
	case err := <-duplicate:
		t.Fatalf("duplicate returned before prefix repair: %v", err)
	default:
	}
	allowOnce.Do(func() { close(allowCatchup) })
	select {
	case err := <-second:
		if err != nil {
			t.Fatalf("proposal after repair: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("proposal did not finish after repair")
	}
	if cluster.cores["n2"].Tip() != 2 || cluster.materials["n2"].Tip() != 2 {
		t.Fatal("survivor did not apply certified prefix and later decision")
	}
	for i := 0; i < concurrentDuplicates; i++ {
		select {
		case err := <-duplicate:
			if err != nil {
				t.Fatalf("duplicate after repair: %v", err)
			}
		case <-ctx.Done():
			t.Fatal("duplicate delivery deadlocked")
		}
	}
	if n := len(cluster.servers["n2"].syncLimit); n != 0 {
		t.Fatalf("catchup sync tokens leaked: %d", n)
	}
	if _, err := probe.callQuorum(ctx, "n2", request(decision)); err != nil {
		t.Fatalf("late exact decision: %v", err)
	}
	if _, err := cluster.servers["n1"].proposeOnce(ctx, encode("survivor-no-gap")); err != nil {
		t.Fatalf("no-gap proposal with one remote survivor: %v", err)
	}
	if cluster.cores["n2"].Tip() != 3 || cluster.materials["n2"].Tip() != 3 {
		t.Fatal("survivor did not apply no-gap decision")
	}
	if noGapFetch.Load() {
		t.Fatal("no-gap decision unexpectedly fetched a prefix")
	}
}

func TestLearnedPrefixRealReconfigurationControl(t *testing.T) {
	for _, futureControl := range []bool{false, true} {
		name := "past-control"
		if futureControl {
			name = "future-control"
		}
		t.Run(name, func(t *testing.T) {
			cluster := newForegroundAPIPeers(t, t.TempDir(), true)
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			if futureControl {
				item, err := types.EncodeKVBatchItem(types.KVCommand{RequestID: "control-preceding", Operation: "put", Key: "control-preceding", Value: []byte("value"), ObservedAtUnixMS: time.Now().UnixMilli()})
				if err != nil {
					t.Fatal(err)
				}
				if _, err = cluster.servers["n1"].proposeOnce(ctx, types.AssembleKVBatch([][]byte{item})); err != nil {
					t.Fatalf("ordinary slot: %v", err)
				}
			}
			target := cluster.config
			target.ConfigID++
			target.Members = []quepaxa.Member{cluster.config.Members[0], cluster.config.Members[2]}
			slot, err := cluster.cores["n1"].BeginReconfiguration(ctx, target)
			if err != nil {
				t.Fatalf("begin reconfiguration: %v", err)
			}
			want := quepaxa.Slot(1)
			if futureControl {
				want = 2
			}
			if slot != want {
				t.Fatalf("control slot=%d want=%d", slot, want)
			}
			values, _, err := cluster.cores["n1"].DecisionsFrom(slot, 1)
			if err != nil || len(values) != 1 {
				t.Fatalf("read control: %v", err)
			}
			isControl, err := quepaxa.DecodeReconfiguration(values[0].Value)
			if err != nil || !isControl {
				t.Fatalf("not a real control: %v", err)
			}
			walPath := t.TempDir()
			wal, err := qlog.Open(walPath)
			if err != nil {
				t.Fatal(err)
			}
			defer wal.Close()
			fresh, err := quepaxa.New(quepaxa.Config{NodeID: "n2", Cluster: cluster.config, WAL: wal, Transport: cluster.transports[1], EnableReconfiguration: true})
			if err != nil {
				t.Fatal(err)
			}
			server := NewServer(fresh, nil, "foreground-api", true, cluster.transports[1])
			defer server.Close()
			err, fatal := server.catchUpLearnedPrefix(ctx, "n1", 1)
			if err != nil || fatal {
				t.Fatalf("catchup: %v fatal=%v", err, fatal)
			}
			if err := fresh.EnsureDurableThrough(ctx, 1); err != nil {
				t.Fatalf("prefix not durable: %v", err)
			}
			if !fresh.IsDecided(1) {
				t.Fatal("required prefix not installed")
			}
			if futureControl && fresh.IsDecided(2) {
				t.Fatal("future control installed beyond target")
			}
			server.Close()
			if err := wal.Close(); err != nil {
				t.Fatalf("close target WAL: %v", err)
			}
			reopened, err := qlog.Open(walPath)
			if err != nil {
				t.Fatalf("reopen target WAL: %v", err)
			}
			defer reopened.Close()
			recovered, err := quepaxa.New(quepaxa.Config{NodeID: "n2", Cluster: cluster.config, WAL: reopened, Transport: cluster.transports[1], EnableReconfiguration: true})
			if err != nil {
				t.Fatalf("reopen certified prefix: %v", err)
			}
			if !recovered.IsDecided(1) || (futureControl && recovered.IsDecided(2)) {
				t.Fatal("reopened WAL has wrong control prefix")
			}
		})
	}
}

func TestLearnedMultipleActivePeerFetchesCancelAndJoin(t *testing.T) {
	cluster := newForegroundAPIPeers(t, t.TempDir())
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	item, err := types.EncodeKVBatchItem(types.KVCommand{RequestID: "multi-fetch", Operation: "put", Key: "multi-fetch", Value: []byte("value"), ObservedAtUnixMS: time.Now().UnixMilli()})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := cluster.servers["n1"].proposeOnce(ctx, types.AssembleKVBatch([][]byte{item})); err != nil {
		t.Fatal(err)
	}
	wal, err := qlog.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()
	core, err := quepaxa.New(quepaxa.Config{NodeID: "n2", Cluster: cluster.config, WAL: wal, Transport: cluster.transports[1]})
	if err != nil {
		t.Fatal(err)
	}
	server := NewServer(core, nil, "foreground-api", true, cluster.transports[1])
	defer server.Close()
	entered := make(chan string, 2)
	release := make(chan struct{})
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(release) })
	restore := localtesthooks.Set(func(event string) {
		if strings.Contains(event, "network:sync:phase=entered:") && strings.Contains(event, ":sender=n2:from=1") {
			if strings.Contains(event, "node=n1:") || strings.Contains(event, "node=n3:") {
				entered <- event
				<-release
			}
		}
	})
	defer restore()
	work, stop := context.WithCancel(ctx)
	defer stop()
	result := make(chan error, 3)
	for _, source := range []quepaxa.NodeID{"n1", "n3", "n1"} {
		go func(id quepaxa.NodeID) { err, _ := server.catchUpLearnedPrefix(work, id, 1); result <- err }(source)
	}
	seen := map[string]bool{}
	for i := 0; i < 2; i++ {
		select {
		case event := <-entered:
			if strings.Contains(event, "node=n1:") {
				seen["n1"] = true
			}
			if strings.Contains(event, "node=n3:") {
				seen["n3"] = true
			}
		case <-ctx.Done():
			t.Fatal("two peer fetches did not enter")
		}
	}
	if !seen["n1"] || !seen["n3"] {
		t.Fatalf("not distinct peers: %v", seen)
	}
	if n := len(server.syncLimit); n != 2 {
		t.Fatalf("active fetch admission=%d, want 2", n)
	}
	stop()
	for i := 0; i < 3; i++ {
		select {
		case err := <-result:
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("cancelled fetch=%v", err)
			}
		case <-ctx.Done():
			t.Fatal("fetch child did not join")
		}
	}
	if n := len(server.syncLimit); n != 0 {
		t.Fatalf("sync tokens leaked: %d", n)
	}
	for _, source := range []quepaxa.NodeID{"n1", "n3"} {
		unlock, err := server.lockCatchUp(ctx, source)
		if err != nil {
			t.Fatalf("source %s leaked: %v", source, err)
		}
		unlock()
	}
	releaseOnce.Do(func() { close(release) })
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for len(cluster.peerServers[0].streams) != 0 || len(cluster.peerServers[2].streams) != 0 {
		select {
		case <-ticker.C:
		case <-ctx.Done():
			t.Fatal("source peer streams did not drain")
		}
	}
	closed := make(chan struct{})
	go func() { server.Close(); close(closed) }()
	select {
	case <-closed:
	case <-ctx.Done():
		t.Fatal("server close did not finish")
	}
}

func TestLearnedCancelledCompetingFetchesReleaseResources(t *testing.T) {
	cluster := newForegroundAPIPeers(t, t.TempDir())
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	server := cluster.servers["n2"]
	unlock, err := server.lockCatchUp(ctx, "n1")
	if err != nil {
		t.Fatal(err)
	}
	var once sync.Once
	defer once.Do(unlock)
	const workers = 16
	workerCtx, stop := context.WithCancel(ctx)
	results := make(chan error, workers)
	for i := 0; i < workers; i++ {
		go func() { err, _ := server.catchUpLearnedPrefix(workerCtx, "n1", 1); results <- err }()
	}
	stop()
	for i := 0; i < workers; i++ {
		select {
		case err := <-results:
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("cancelled fetch returned %v", err)
			}
		case <-ctx.Done():
			t.Fatal("fetch worker leaked")
		}
	}
	once.Do(unlock)
	reacquire, err := server.lockCatchUp(ctx, "n1")
	if err != nil {
		t.Fatalf("source admission leaked: %v", err)
	}
	reacquire()
	if n := len(server.syncLimit); n != 0 {
		t.Fatalf("sync tokens leaked: %d", n)
	}
}

func TestLearnedPrefixImportStopsAtTarget(t *testing.T) {
	cluster := newForegroundAPIPeers(t, t.TempDir())
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	release := make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(release) })
	var dropped sync.WaitGroup
	dropped.Add(2)
	restore := localtesthooks.Set(func(event string) {
		if strings.Contains(event, "network:learned:phase=received:node=n2:") && (strings.Contains(event, ":slot=1") || strings.Contains(event, ":slot=2")) {
			dropped.Done()
			<-release
			runtime.Goexit()
		}
	})
	for i := 1; i <= 2; i++ {
		id := fmt.Sprintf("bounded-prefix-%d", i)
		item, err := types.EncodeKVBatchItem(types.KVCommand{RequestID: id, Operation: "put", Key: id, Value: []byte(id), ObservedAtUnixMS: time.Now().UnixMilli()})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := cluster.servers["n1"].proposeOnce(ctx, types.AssembleKVBatch([][]byte{item})); err != nil {
			t.Fatal(err)
		}
	}
	ready := make(chan struct{})
	go func() { dropped.Wait(); close(ready) }()
	select {
	case <-ready:
	case <-ctx.Done():
		t.Fatal("deliveries not blocked")
	}
	once.Do(func() { close(release) })
	restore()
	err, fatal := cluster.servers["n2"].catchUpLearnedPrefix(ctx, "n1", 1)
	if err != nil || fatal {
		t.Fatalf("bounded prefix: err=%v fatal=%t", err, fatal)
	}
	if !cluster.cores["n2"].IsDecided(1) || cluster.cores["n2"].IsDecided(2) {
		t.Fatal("prefix import did not stop at target")
	}
	if err := cluster.cores["n2"].EnsureDurableThrough(ctx, 1); err != nil {
		t.Fatalf("prefix durability: %v", err)
	}
}

func TestLearnedFetchedPageDiscardedAfterCompetingPrefix(t *testing.T) {
	cluster := newForegroundAPIPeers(t, t.TempDir())
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	value := func(id string) []byte {
		t.Helper()
		item, err := types.EncodeKVBatchItem(types.KVCommand{RequestID: id, Operation: "put", Key: id, Value: []byte(id), ObservedAtUnixMS: time.Now().UnixMilli()})
		if err != nil {
			t.Fatal(err)
		}
		return types.AssembleKVBatch([][]byte{item})
	}
	values := [][]byte{value("page-discard-1"), value("page-discard-2")}
	var drops sync.WaitGroup
	drops.Add(2)
	releaseDrops := make(chan struct{})
	var dropsOnce sync.Once
	defer dropsOnce.Do(func() { close(releaseDrops) })
	restore := localtesthooks.Set(func(event string) {
		if strings.Contains(event, "network:learned:phase=received:node=n2:") && (strings.Contains(event, ":slot=1") || strings.Contains(event, ":slot=2")) {
			drops.Done()
			<-releaseDrops
			runtime.Goexit()
		}
	})
	for _, v := range values {
		if _, err := cluster.servers["n1"].proposeOnce(ctx, v); err != nil {
			t.Fatal(err)
		}
	}
	dropped := make(chan struct{})
	go func() { drops.Wait(); close(dropped) }()
	select {
	case <-dropped:
	case <-ctx.Done():
		t.Fatal("learned deliveries not gated")
	}
	dropsOnce.Do(func() { close(releaseDrops) })
	restore()
	pageReady, releasePage := make(chan struct{}, 1), make(chan struct{})
	var pageOnce sync.Once
	defer pageOnce.Do(func() { close(releasePage) })
	var imports atomic.Int32
	restorePage := localtesthooks.Set(func(event string) {
		if event == "core:certified-values:before-sync:node=n2" {
			imports.Add(1)
		}
		if strings.Contains(event, "network:learned:prefix-before-install:node=n2:from=1:through=1") {
			select {
			case pageReady <- struct{}{}:
			default:
			}
			<-releasePage
		}
	})
	defer restorePage()
	result := make(chan error, 1)
	go func() { err, _ := cluster.servers["n2"].catchUpLearnedPrefix(ctx, "n1", 1); result <- err }()
	select {
	case <-pageReady:
	case <-ctx.Done():
		t.Fatal("page not fetched")
	}
	otherPage, err := cluster.transports[1].FetchDecisions(ctx, "n3", 1, 128)
	if err != nil || len(otherPage.Decisions) < 1 {
		t.Fatalf("competing peer fetch: %v", err)
	}
	if err := cluster.cores["n2"].AcceptCertifiedValues(otherPage.Decisions[:1]); err != nil {
		t.Fatalf("competing certified prefix: %v", err)
	}
	before := imports.Load()
	if before != 1 {
		t.Fatalf("competing import count %d", before)
	}
	pageOnce.Do(func() { close(releasePage) })
	select {
	case err := <-result:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("page owner failed to finish")
	}
	if imports.Load() != before {
		t.Fatal("already-filled page installed redundantly")
	}
	if cluster.cores["n2"].IsDecided(2) {
		t.Fatal("unneeded future slot installed")
	}
}

func TestLearnedFetchSyncCannotBeCancelledByTip(t *testing.T) {
	testLearnedSyncGate(t, "normal")
}

func TestLearnedCloseWaitsForOwnedInstall(t *testing.T) {
	testLearnedSyncGate(t, "close")
}

func TestLearnedSyncErrorNeverAcknowledges(t *testing.T) {
	testLearnedSyncGate(t, "sync-error")
}

func testLearnedSyncGate(t *testing.T, mode string) {
	cluster := newForegroundAPIPeers(t, t.TempDir())
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	encode := func(id string) []byte {
		t.Helper()
		item, err := types.EncodeKVBatchItem(types.KVCommand{RequestID: id, Operation: "put", Key: id, Value: []byte(id), ObservedAtUnixMS: time.Now().UnixMilli()})
		if err != nil {
			t.Fatal(err)
		}
		return types.AssembleKVBatch([][]byte{item})
	}
	firstValue, secondValue := encode("sync-gate-first"), encode("sync-gate-second")
	dropped, releaseDrop := make(chan struct{}, 1), make(chan struct{})
	var dropOnce sync.Once
	defer dropOnce.Do(func() { close(releaseDrop) })
	restore := localtesthooks.Set(func(event string) {
		if strings.Contains(event, "network:learned:phase=received:node=n2:") && strings.Contains(event, ":slot=1") {
			select {
			case dropped <- struct{}{}:
			default:
			}
			<-releaseDrop
			runtime.Goexit()
		}
	})
	first := make(chan error, 1)
	go func() { _, err := cluster.servers["n1"].proposeOnce(ctx, firstValue); first <- err }()
	select {
	case <-dropped:
	case <-ctx.Done():
		t.Fatal("lower Learned not gated")
	}
	select {
	case err := <-first:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("first proposal timed out")
	}
	dropOnce.Do(func() { close(releaseDrop) })
	restore()
	if err := cluster.peerServers[2].Close(); err != nil {
		t.Fatal(err)
	}
	blocked, releaseSync := make(chan struct{}, 1), make(chan struct{})
	installFailed := make(chan string, 1)
	var syncOnce sync.Once
	defer syncOnce.Do(func() { close(releaseSync) })
	restoreSync := localtesthooks.Set(func(event string) {
		if strings.Contains(event, "network:learned:phase=prefix-install-failed:node=n2:slot=2") {
			select {
			case installFailed <- event:
			default:
			}
		}
		if event == "core:certified-values:before-sync:node=n2" {
			select {
			case blocked <- struct{}{}:
			default:
			}
			<-releaseSync
		}
	})
	defer restoreSync()
	second := make(chan error, 1)
	go func() { _, err := cluster.servers["n1"].proposeOnce(ctx, secondValue); second <- err }()
	select {
	case <-blocked:
	case <-ctx.Done():
		t.Fatal("certified catchup sync not reached")
	}
	if cluster.cores["n2"].Tip() < 1 {
		t.Fatal("prefix did not advance before sync")
	}
	var closed chan error
	if mode == "close" {
		closed = make(chan error, 1)
		go func() { closed <- cluster.peerServers[1].Close() }()
		select {
		case err := <-closed:
			t.Fatalf("peer closed while owned install blocked: %v", err)
		case <-time.After(200 * time.Millisecond):
		}
	}
	select {
	case err := <-second:
		t.Fatalf("ACK before sync release: %v", err)
	case <-time.After(200 * time.Millisecond):
	}
	if mode == "sync-error" {
		// Close only this test-owned WAL after append and before its next sync.
		if err := cluster.wals[1].Close(); err != nil {
			t.Fatalf("close injected WAL: %v", err)
		}
	}
	syncOnce.Do(func() { close(releaseSync) })
	if mode == "sync-error" {
		select {
		case event := <-installFailed:
			if !strings.Contains(event, "closed") {
				t.Fatalf("unexpected install error: %s", event)
			}
		case <-ctx.Done():
			t.Fatal("WAL sync error not observed")
		}
		select {
		case err := <-second:
			if err == nil {
				t.Fatal("ACK after failed WAL sync")
			}
		case <-ctx.Done():
		}
		if cluster.materials["n2"].Tip() != 0 {
			t.Fatal("applied despite failed WAL sync")
		}
		return
	}
	if mode == "close" {
		select {
		case err := <-closed:
			if err != nil {
				t.Fatalf("close after install: %v", err)
			}
		case <-ctx.Done():
			t.Fatal("peer close did not drain after install")
		}
		select {
		case err := <-second:
			if err == nil {
				t.Fatal("proposal ACK after peer close")
			}
		case <-ctx.Done():
			t.Fatal("proposal did not terminate after peer close")
		}
		return
	}
	select {
	case err := <-second:
		if err != nil {
			t.Fatalf("after sync: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("did not complete after sync")
	}
	if cluster.materials["n2"].Tip() != 2 {
		t.Fatal("ACK without applying both values")
	}
}

func TestLearnedCompetingRecoveryCancelsBlockedSenderFetch(t *testing.T) {
	cluster := newForegroundAPIPeers(t, t.TempDir())
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	encode := func(id string) []byte {
		t.Helper()
		item, err := types.EncodeKVBatchItem(types.KVCommand{RequestID: id, Operation: "put", Key: id, Value: []byte(id), ObservedAtUnixMS: time.Now().UnixMilli()})
		if err != nil {
			t.Fatal(err)
		}
		return types.AssembleKVBatch([][]byte{item})
	}
	firstValue, secondValue := encode("competing-first"), encode("competing-second")
	dropped := make(chan struct{}, 1)
	releaseDrop := make(chan struct{})
	var dropOnce sync.Once
	defer dropOnce.Do(func() { close(releaseDrop) })
	restore := localtesthooks.Set(func(event string) {
		if strings.Contains(event, "network:learned:phase=received:node=n2:") && strings.Contains(event, ":slot=1") {
			select {
			case dropped <- struct{}{}:
			default:
			}
			<-releaseDrop
			runtime.Goexit()
		}
	})
	first := make(chan error, 1)
	go func() { _, err := cluster.servers["n1"].proposeOnce(ctx, firstValue); first <- err }()
	select {
	case <-dropped:
	case <-ctx.Done():
		t.Fatal("first delivery not blocked")
	}
	select {
	case err := <-first:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("first quorum timeout")
	}
	dropOnce.Do(func() { close(releaseDrop) })
	restore()
	unlock, err := cluster.servers["n2"].lockCatchUp(ctx, "n1")
	if err != nil {
		t.Fatal(err)
	}
	var unlockOnce sync.Once
	defer unlockOnce.Do(unlock)
	started := make(chan struct{}, 2)
	restore2 := localtesthooks.Set(func(event string) {
		if strings.Contains(event, "network:learned:phase=prefix-catchup-start:node=n2:") && strings.Contains(event, ":slot=2") {
			select {
			case started <- struct{}{}:
			default:
			}
		}
	})
	defer restore2()
	second := make(chan error, 1)
	go func() { _, err := cluster.servers["n1"].proposeOnce(ctx, secondValue); second <- err }()
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal("sender catchup not attempted")
	}
	values, _, err := cluster.cores["n1"].DecisionsFrom(2, 1)
	if err != nil || len(values) != 1 {
		t.Fatalf("certified slot 2: %v", err)
	}
	var decision quepaxa.Decision
	if err := json.Unmarshal(values[0].Certificate, &decision); err != nil {
		t.Fatal(err)
	}
	decision.Proposal.Value = values[0].Value
	probe := cluster.transports[0]
	request := probe.request(peerfb.OperationLearned)
	request.Decision = decisionToWire(decision)
	direct := make(chan error, 1)
	go func() { _, err := probe.callQuorum(ctx, "n2", request); direct <- err }()
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal("independent sender catchup not attempted")
	}
	select {
	case err := <-direct:
		t.Fatalf("premature ACK: %v", err)
	default:
	}
	if err := cluster.servers["n2"].catchUpFrom(ctx, "n3", 1, true); err != nil {
		t.Fatalf("alternative source: %v", err)
	}
	select {
	case err := <-direct:
		if err != nil {
			t.Fatalf("after concurrent repair: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("still waiting on blocked sender after alternative repair")
	}
	if cluster.materials["n2"].Tip() != 2 {
		t.Fatal("ACK before applying certified decision")
	}
	unlockOnce.Do(unlock)
	select {
	case <-second:
	case <-ctx.Done():
		t.Fatal("proposal did not complete")
	}
}

func TestLearnedSenderUnavailableOtherPeerRepairsPrefix(t *testing.T) {
	cluster := newForegroundAPIPeers(t, t.TempDir())
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	encode := func(id string) []byte {
		t.Helper()
		item, err := types.EncodeKVBatchItem(types.KVCommand{RequestID: id, Operation: "put", Key: id, Value: []byte(id), ObservedAtUnixMS: time.Now().UnixMilli()})
		if err != nil {
			t.Fatal(err)
		}
		return types.AssembleKVBatch([][]byte{item})
	}
	dropped := make(chan struct{}, 1)
	releaseDrop := make(chan struct{})
	var releaseDropOnce sync.Once
	defer releaseDropOnce.Do(func() { close(releaseDrop) })
	restore := localtesthooks.Set(func(event string) {
		if strings.Contains(event, "network:learned:phase=received:node=n2:") && strings.Contains(event, ":slot=1") {
			select {
			case dropped <- struct{}{}:
			default:
			}
			<-releaseDrop
			runtime.Goexit()
		}
	})
	first := make(chan error, 1)
	go func() { _, err := cluster.servers["n1"].proposeOnce(ctx, encode("alternative-first")); first <- err }()
	select {
	case <-dropped:
	case <-ctx.Done():
		t.Fatal("first delivery not blocked")
	}
	select {
	case err := <-first:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("first quorum timeout")
	}
	releaseDropOnce.Do(func() { close(releaseDrop) })
	restore()
	started, failed, response := make(chan struct{}, 1), make(chan struct{}, 2), make(chan struct{}, 1)
	allow := make(chan struct{})
	var allowOnce sync.Once
	defer allowOnce.Do(func() { close(allow) })
	restore2 := localtesthooks.Set(func(event string) {
		if !strings.Contains(event, "node=n2:") || !strings.Contains(event, ":slot=2") {
			return
		}
		if strings.Contains(event, "phase=prefix-catchup-start") {
			select {
			case started <- struct{}{}:
			default:
			}
			<-allow
		}
		if strings.Contains(event, "phase=prefix-catchup-failed") {
			select {
			case failed <- struct{}{}:
			default:
			}
		}
		if strings.Contains(event, "phase=response-written") {
			select {
			case response <- struct{}{}:
			default:
			}
		}
	})
	defer restore2()
	second := make(chan error, 1)
	go func() { _, err := cluster.servers["n1"].proposeOnce(ctx, encode("alternative-second")); second <- err }()
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal("second catchup not started")
	}
	values, _, err := cluster.cores["n1"].DecisionsFrom(2, 1)
	if err != nil || len(values) != 1 {
		t.Fatalf("load certified slot 2: %v", err)
	}
	var decision quepaxa.Decision
	if err := json.Unmarshal(values[0].Certificate, &decision); err != nil {
		t.Fatal(err)
	}
	decision.Proposal.Value = values[0].Value
	probe := cluster.transports[0]
	request := probe.request(peerfb.OperationLearned)
	request.Decision = decisionToWire(decision)
	direct := make(chan error, 1)
	go func() { _, err := probe.callQuorum(ctx, "n2", request); direct <- err }()
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal("independent delivery not started")
	}
	if !cluster.cores["n3"].IsDecided(1) {
		t.Fatal("alternative peer lacks certified prefix")
	}
	if err := cluster.peerServers[0].Close(); err != nil {
		t.Fatal(err)
	}
	allowOnce.Do(func() { close(allow) })
	for i := 0; i < 2; i++ {
		select {
		case <-failed:
		case <-ctx.Done():
			t.Fatalf("sender fetch failure %d/2 not observed", i)
		}
	}
	select {
	case <-response:
		t.Fatal("ACK before alternative recovery")
	default:
	}
	select {
	case err := <-direct:
		t.Fatalf("independent delivery finished before repair: %v", err)
	default:
	}
	if err := cluster.servers["n2"].catchUpFrom(ctx, "n3", 1, true); err != nil {
		t.Fatalf("alternative certified recovery: %v", err)
	}
	select {
	case err := <-direct:
		if err != nil {
			t.Fatalf("independent delivery after repair: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("no ACK after alternative recovery")
	}
	if cluster.cores["n2"].Tip() != 2 || cluster.materials["n2"].Tip() != 2 {
		t.Fatal("alternative recovery failed to apply complete certified prefix")
	}
	select {
	case <-second:
	case <-ctx.Done():
		t.Fatal("proposal did not complete")
	}
}

func TestLearnGapBlocksQuorumUntilCatchUpInstallsPrefix(t *testing.T) {
	cluster := newForegroundAPIPeers(t, t.TempDir())
	const lower = quepaxa.Slot(1)
	const gap = quepaxa.Slot(2)
	const higher = quepaxa.Slot(3)
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()

	value := func(id string) []byte {
		t.Helper()
		item, err := types.EncodeKVBatchItem(types.KVCommand{
			RequestID: id, Operation: "put", Key: id, Value: []byte(id), ObservedAtUnixMS: time.Now().UnixMilli(),
		})
		if err != nil {
			t.Fatal(err)
		}
		return types.AssembleKVBatch([][]byte{item})
	}
	wrapRestore := func(restore func()) func() {
		var once sync.Once
		return func() { once.Do(restore) }
	}
	dropLearned := func(slot quepaxa.Slot, nodes ...string) (<-chan string, <-chan string, func(), func()) {
		dropped := make(chan string, len(nodes))
		stopped := make(chan string, len(nodes))
		release := make(chan struct{})
		var releaseOnce sync.Once
		releaseHandlers := func() { releaseOnce.Do(func() { close(release) }) }
		restore := localtesthooks.Set(func(event string) {
			if !strings.Contains(event, "network:learned:phase=received:") || !strings.Contains(event, fmt.Sprintf(":slot=%d", slot)) {
				return
			}
			for _, node := range nodes {
				if strings.Contains(event, "node="+node+":") {
					dropped <- event
					<-release // Hold this real handler until the sender's outcome is known.
					stopped <- event
					runtime.Goexit() // Drop before Core.AcceptDecision, after explicit observation.
				}
			}
		})
		return dropped, stopped, releaseHandlers, wrapRestore(restore)
	}
	waitDrops := func(ch <-chan string, count int) {
		t.Helper()
		for i := 0; i < count; i++ {
			select {
			case <-ch:
			case <-ctx.Done():
				t.Fatalf("received %d/%d injected learned drops: %v", i, count, ctx.Err())
			}
		}
	}
	catchUp := func(node string, through quepaxa.Slot) error {
		return cluster.servers[quepaxa.NodeID(node)].catchUpFrom(ctx, "n1", through, true)
	}
	assertTips := func(node string, want quepaxa.Slot) {
		t.Helper()
		if got := cluster.cores[quepaxa.NodeID(node)].Tip(); got != want {
			t.Fatalf("%s core tip=%d, want %d", node, got, want)
		}
		if got := cluster.materials[quepaxa.NodeID(node)].Tip(); got != uint64(want) {
			t.Fatalf("%s materializer tip=%d, want %d", node, got, want)
		}
	}

	// Control: one laggard misses a lower decision, but the other remote ACK
	// completes quorum. This establishes that a single gap is not a quorum stall.
	drops, stopped, release, restore := dropLearned(lower, "n3")
	defer func() { release(); restore() }()
	controlCtx, cancelControl := context.WithTimeout(ctx, 2*time.Second)
	controlDone := make(chan error, 1)
	go func() {
		_, err := cluster.servers["n1"].proposeOnce(controlCtx, value("gap-single-laggard-control"))
		controlDone <- err
	}()
	waitDrops(drops, 1)
	controlErr := <-controlDone
	release()
	select {
	case <-stopped:
	case <-ctx.Done():
		t.Fatalf("single laggard handler did not stop: %v", ctx.Err())
	}
	cancelControl()
	restore()
	if controlErr != nil {
		t.Fatalf("single-laggard quorum control: %v", controlErr)
	}
	if cluster.cores["n3"].Tip() != 0 || cluster.cores["n3"].IsDecided(lower) {
		t.Fatalf("control laggard unexpectedly learned slot %d: tip=%d", lower, cluster.cores["n3"].Tip())
	}
	if err := catchUp("n3", lower); err != nil {
		t.Fatalf("repair control laggard: %v", err)
	}
	for _, node := range []string{"n1", "n2", "n3"} {
		if err := cluster.servers[quepaxa.NodeID(node)].applyDecisions(ctx, lower); err != nil {
			t.Fatalf("apply control decision at %s: %v", node, err)
		}
		assertTips(node, lower)
	}

	// A second locally decided value whose learned RPC is dropped by both
	// receivers leaves a real certified prefix gap. Keep this failure visible;
	// do not turn the lower delivery into a success or retry it.
	drops, stopped, releaseLower, restoreLower := dropLearned(gap, "n2", "n3")
	defer func() { releaseLower(); restoreLower() }()
	failureCtx, cancelFailure := context.WithTimeout(ctx, 300*time.Millisecond)
	failureDone := make(chan error, 1)
	go func() {
		_, err := cluster.servers["n1"].proposeOnce(failureCtx, value("gap-both-laggards"))
		failureDone <- err
	}()
	waitDrops(drops, 2)
	lowerErr := <-failureDone
	cancelFailure()
	if lowerErr == nil || (!errors.Is(lowerErr, context.DeadlineExceeded) && !errors.Is(lowerErr, quepaxa.ErrQuorumUnavailable)) {
		t.Fatalf("uncertified learned delivery error=%v, want bounded quorum failure", lowerErr)
	}
	if !cluster.cores["n1"].IsDecided(gap) {
		t.Fatalf("source did not locally certify gap slot %d after failed delivery", gap)
	}
	for _, node := range []string{"n2", "n3"} {
		if cluster.cores[quepaxa.NodeID(node)].Tip() != lower || cluster.cores[quepaxa.NodeID(node)].IsDecided(gap) {
			t.Fatalf("%s did not retain the expected prefix gap: tip=%d decided=%t", node, cluster.cores[quepaxa.NodeID(node)].Tip(), cluster.cores[quepaxa.NodeID(node)].IsDecided(gap))
		}
	}
	// Restore the lower hook before installing the higher-slot observer. The
	// already-blocked handlers retain the callback closure until released.
	restoreLower()

	started := make(chan string, 2)
	allowDirect := make(chan struct{})
	var allowDirectOnce sync.Once
	defer allowDirectOnce.Do(func() { close(allowDirect) })
	events := make(chan string, 24)
	restoreHigher := wrapRestore(localtesthooks.Set(func(event string) {
		if strings.Contains(event, ":slot=3") && (strings.Contains(event, "node=n2:") || strings.Contains(event, "node=n3:")) {
			if strings.Contains(event, "phase=prefix-catchup-start") {
				started <- event
				<-allowDirect
			} else if strings.Contains(event, "phase=apply-complete") || strings.Contains(event, "phase=response-written") {
				events <- event
			}
		}
	}))
	defer restoreHigher()
	proposalDone := make(chan error, 1)
	go func() {
		_, err := cluster.servers["n1"].proposeOnce(ctx, value("gap-higher"))
		proposalDone <- err
	}()
	startedNodes := map[string]bool{}
	for len(startedNodes) != 2 {
		select {
		case event := <-started:
			if strings.Contains(event, "node=n2:") {
				startedNodes["n2"] = true
			} else if strings.Contains(event, "node=n3:") {
				startedNodes["n3"] = true
			}
		case err := <-proposalDone:
			t.Fatalf("higher proposal returned before both lagging handlers reached apply: %v", err)
		case <-ctx.Done():
			t.Fatalf("higher handlers did not reach apply: %v", ctx.Err())
		}
	}
	for _, node := range []string{"n2", "n3"} {
		core := cluster.cores[quepaxa.NodeID(node)]
		if core.Tip() != lower || !core.IsDecided(higher) || core.IsDecided(gap) {
			t.Fatalf("%s expected higher decision blocked behind gap: tip=%d gap=%t higher=%t", node, core.Tip(), core.IsDecided(gap), core.IsDecided(higher))
		}
	}
	select {
	case err := <-proposalDone:
		t.Fatalf("higher SendDecision returned before prefix repair: %v", err)
	default:
	}

	// Block direct repair to prove no ACK precedes certified prefix installation.
	repairErr := make(chan error, 2)
	for _, node := range []string{"n2", "n3"} {
		go func(node string) { repairErr <- catchUp(node, higher) }(node)
	}
	for range 2 {
		select {
		case err := <-repairErr:
			if err != nil {
				t.Fatalf("concurrent prefix catch-up: %v", err)
			}
		case <-ctx.Done():
			t.Fatalf("prefix catch-up did not complete: %v", ctx.Err())
		}
	}
	allowDirectOnce.Do(func() { close(allowDirect) })
	for _, node := range []string{"n2", "n3"} {
		core := cluster.cores[quepaxa.NodeID(node)]
		if core.Tip() != higher || !core.IsDecided(gap) || !core.IsDecided(higher) {
			t.Fatalf("%s catch-up did not install the missing prefix: tip=%d gap=%t higher=%t", node, core.Tip(), core.IsDecided(gap), core.IsDecided(higher))
		}
	}
	select {
	case err := <-proposalDone:
		if err != nil {
			t.Fatalf("higher quorum after catch-up: %v", err)
		}
	case <-ctx.Done():
		t.Fatalf("higher quorum remained blocked after apply release: %v", ctx.Err())
	}

	applyComplete := map[string]bool{}
	responseWritten := map[string]bool{}
	for len(responseWritten) == 0 {
		select {
		case event := <-events:
			if strings.Contains(event, "phase=apply-complete") {
				if strings.Contains(event, "node=n2:") {
					applyComplete["n2"] = true
				}
				if strings.Contains(event, "node=n3:") {
					applyComplete["n3"] = true
				}
			}
			if strings.Contains(event, "phase=response-written") {
				if strings.Contains(event, "node=n2:") {
					responseWritten["n2"] = true
				}
				if strings.Contains(event, "node=n3:") {
					responseWritten["n3"] = true
				}
			}
		case <-ctx.Done():
			t.Fatalf("missing apply/ACK evidence: apply=%v response=%v", applyComplete, responseWritten)
		}
	}
	// SendDecision needs one remote ACK after local vote; quorum cancellation can
	// prevent the other response from being written. Any observed ACK must follow
	// that peer's apply-complete event; ACK-before-apply is never accepted.
	for node := range responseWritten {
		if !applyComplete[node] {
			t.Fatalf("%s wrote learned ACK before apply completion", node)
		}
		if got := cluster.materials[quepaxa.NodeID(node)].Tip(); got != uint64(higher) {
			t.Fatalf("ACKing peer %s materializer tip=%d, want %d", node, got, higher)
		}
	}
	for node := range applyComplete {
		if got := cluster.materials[quepaxa.NodeID(node)].Tip(); got != uint64(higher) {
			t.Fatalf("peer %s reported apply-complete with materializer tip=%d, want %d", node, got, higher)
		}
	}
	if len(responseWritten) == 0 {
		t.Fatal("no post-apply learned ACK was observed")
	}
	if err := cluster.servers["n1"].applyDecisions(ctx, higher); err != nil {
		t.Fatalf("apply source decisions: %v", err)
	}
	assertTips("n1", higher)
	releaseLower()
	for range 2 {
		select {
		case <-stopped:
		case <-ctx.Done():
			t.Fatalf("lower learned handler did not stop after catch-up: %v", ctx.Err())
		}
	}
}

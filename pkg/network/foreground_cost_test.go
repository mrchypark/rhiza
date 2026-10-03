package network

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	objmetrics "github.com/mrchypark/rhiza/internal/objstore"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/checkpoint"
	"github.com/mrchypark/rhiza/pkg/materializer"
	"github.com/mrchypark/rhiza/pkg/qlog"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/mrchypark/rhiza/pkg/recovery"
	"github.com/quic-go/quic-go"
)

const (
	foregroundAPIWorkers                   = 16
	foregroundAPIKeys                      = 4096
	foregroundAPIValue                     = 1024
	foregroundAPISeedLogEvery              = 512
	foregroundAPIWindowTotal               = 210 * time.Second // 30s warm-up + 120s measurement + 2x30s bounded drains
	foregroundAPIWindowCleanupReserve      = 15 * time.Second
	foregroundAPICallMaxDuration           = 30 * time.Second
	foregroundCheckpointBarrierMaxAttempts = 50
	foregroundCheckpointBarrierRetryDelay  = 20 * time.Millisecond
)

var errForegroundAPISeedBudgetExpired = errors.New("foreground API seed budget expired")

type foregroundAPIPeers struct {
	closeOnce   sync.Once
	config      quepaxa.Cluster
	cores       map[quepaxa.NodeID]*quepaxa.Core
	servers     map[quepaxa.NodeID]*Server
	materials   map[quepaxa.NodeID]*materializer.Materializer
	checkpoints map[quepaxa.NodeID]*checkpoint.Manager
	transports  []*Transport
	peerServers []*PeerServer
	listeners   []*quic.Transport
	connections []net.PacketConn
	wals        []*qlog.WAL
}

type foregroundCheckpointBarrierStats struct {
	attempts          atomic.Uint64
	retries           atomic.Uint64
	admissionRefusals atomic.Uint64
	successes         atomic.Uint64
	failures          atomic.Uint64
}

type foregroundLearnObservation struct {
	mu    sync.Mutex
	first string
	count int
}

func (o *foregroundLearnObservation) capture(event string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.first == "" && strings.HasPrefix(event, "network:foreground-learn-failure:") {
		o.first = event
	}
	if strings.HasPrefix(event, "network:foreground-learn-failure:") {
		o.count++
	}
}

func (o *foregroundLearnObservation) eventCount() int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.count
}

func (o *foregroundLearnObservation) firstEvent() string {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.first
}

func foregroundLearnEventField(event, name string) string {
	for _, field := range strings.Split(event, ":") {
		key, value, ok := strings.Cut(field, "=")
		if ok && key == name {
			return value
		}
	}
	return ""
}

func foregroundLearnObservationSummary(peers *foregroundAPIPeers, observation *foregroundLearnObservation) string {
	event := observation.firstEvent()
	if event == "" {
		return "none"
	}
	var expected quepaxa.ValueHash
	hashBytes, err := hex.DecodeString(foregroundLearnEventField(event, "hash"))
	hasHash := err == nil && len(hashBytes) == len(expected)
	if hasHash {
		copy(expected[:], hashBytes)
	}
	slotNumber, err := strconv.ParseUint(foregroundLearnEventField(event, "slot"), 10, 64)
	if err != nil {
		return event + ":snapshot=unavailable"
	}
	slot := quepaxa.Slot(slotNumber)
	snapshots := make([]string, 0, len(peers.cores))
	for _, member := range peers.config.Members {
		core := peers.cores[member.ID]
		decision, ok := core.CertifiedValue(slot)
		state := "missing"
		if ok {
			state = "different-hash"
			if hasHash && decision.Hash == expected {
				state = "matching-hash"
			}
		}
		snapshots = append(snapshots, fmt.Sprintf("%s=%s/tip-%d", member.ID, state, core.Tip()))
	}
	return event + ":observation_time_local_certified=" + strings.Join(snapshots, ",")
}

func newForegroundAPIPeers(t testing.TB, root string) *foregroundAPIPeers {
	t.Helper()
	clusterID := types.ClusterID("foreground-api")
	ids := []quepaxa.NodeID{"n1", "n2", "n3"}
	tokens := make(map[quepaxa.NodeID]string, len(ids))
	peers := &foregroundAPIPeers{
		cores: make(map[quepaxa.NodeID]*quepaxa.Core, len(ids)), servers: make(map[quepaxa.NodeID]*Server, len(ids)),
		materials: make(map[quepaxa.NodeID]*materializer.Materializer, len(ids)), checkpoints: make(map[quepaxa.NodeID]*checkpoint.Manager, len(ids)),
	}
	t.Cleanup(peers.close)
	for _, id := range ids {
		token := "foreground-voter-" + string(id)
		tokens[id] = token
		member := testMember(clusterID, id, token)
		conn, err := net.ListenPacket("udp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		member.PeerURL = "quic://" + conn.LocalAddr().String()
		peers.connections = append(peers.connections, conn)
		peers.config.Members = append(peers.config.Members, member)
	}
	peers.config.ConfigID = 1
	for _, member := range peers.config.Members {
		transport := NewTransport(clusterID, member.ID, &peers.config, tokens[member.ID])
		peers.transports = append(peers.transports, transport)
		nodeDir := filepath.Join(root, string(member.ID))
		if err := os.MkdirAll(nodeDir, 0o700); err != nil {
			t.Fatal(err)
		}
		wal, err := qlog.Open(filepath.Join(nodeDir, "qlog"))
		if err != nil {
			t.Fatal(err)
		}
		peers.wals = append(peers.wals, wal)
		core, err := quepaxa.New(quepaxa.Config{NodeID: member.ID, Cluster: peers.config, WAL: wal, Transport: transport})
		if err != nil {
			_ = wal.Close()
			t.Fatal(err)
		}
		transport.BindCore(core)
		material, err := materializer.Open(filepath.Join(nodeDir, "sqlite.db"), 4)
		if err != nil {
			t.Fatal(err)
		}
		peers.materials[member.ID] = material
		server := NewServer(core, material, clusterID, true, transport)
		peers.cores[member.ID] = core
		peers.servers[member.ID] = server
	}
	for i, member := range peers.config.Members {
		listener := &quic.Transport{Conn: peers.connections[i]}
		peers.listeners = append(peers.listeners, listener)
		peer, err := StartPeerServerOnTransport(context.Background(), listener, peers.servers[member.ID], peers.config.Members, tokens[member.ID], "foreground-admin")
		if err != nil {
			t.Fatal(err)
		}
		peers.peerServers = append(peers.peerServers, peer)
	}
	return peers
}

func (p *foregroundAPIPeers) close() {
	p.closeOnce.Do(func() {
		for _, peer := range p.peerServers {
			_ = peer.Close()
		}
		for _, server := range p.servers {
			server.Close()
		}
		for _, transport := range p.transports {
			_ = transport.Close()
		}
		for _, listener := range p.listeners {
			_ = listener.Close()
		}
		for _, conn := range p.connections {
			_ = conn.Close()
		}
		for _, material := range p.materials {
			_ = material.Close()
		}
		for _, wal := range p.wals {
			_ = wal.Close()
		}
	})
}

type apiLatencySample struct {
	all, success, overlapAll, overlapSuccess []time.Duration
	drainAll, drainSuccess                   []time.Duration
	drainOverlapAll, drainOverlapSuccess     []time.Duration
	started, errors, rejections, timeouts    int
	commitUnknown                            int
	drainErrors, drainRejections             int
	drainTimeouts, drainCommitUnknown        int
	recording, drainRecording                time.Duration
}

type apiLatencyAccumulator struct {
	mu sync.Mutex
	apiLatencySample
}

const (
	foregroundPUTDiagnosticLimit = 16
	foregroundPUTStackLimit      = 512 << 10
	foregroundPUTErrorChainLimit = 12
)

type foregroundPUTErrorNode struct {
	Depth int    `json:"depth"`
	Type  string `json:"type"`
	Error string `json:"error"`
}

type foregroundPUTUnknown struct {
	Slot             quepaxa.Slot `json:"slot"`
	RetryThroughSlot uint64       `json:"retry_through_slot"`
	RequestID        string       `json:"request_id"`
	CauseType        string       `json:"cause_type,omitempty"`
	Cause            string       `json:"cause,omitempty"`
}

type foregroundPUTFailure struct {
	Phase                 string                   `json:"phase"`
	CompletionWindow      string                   `json:"completion_window"`
	RequestID             string                   `json:"request_id"`
	ErrorType             string                   `json:"error_type"`
	Error                 string                   `json:"error"`
	ErrorChain            []foregroundPUTErrorNode `json:"error_chain"`
	ErrorChainTruncated   bool                     `json:"error_chain_truncated"`
	Unknowns              []foregroundPUTUnknown   `json:"commit_unknowns,omitempty"`
	Elapsed               time.Duration            `json:"elapsed_ns"`
	StartedAt             time.Time                `json:"started_at"`
	CompletedAt           time.Time                `json:"completed_at"`
	CutoffKnown           bool                     `json:"cutoff_known"`
	CompletionOffset      time.Duration            `json:"completion_offset_ns"`
	CompletedBeforeCutoff bool                     `json:"completed_before_cutoff"`
	CallDeadline          time.Time                `json:"call_deadline"`
	CallContextErr        string                   `json:"call_context_error"`
	IsDeadline            bool                     `json:"is_deadline"`
	IsCanceled            bool                     `json:"is_canceled"`
	IsQuorumUnavailable   bool                     `json:"is_quorum_unavailable"`
	IsCommitUnknown       bool                     `json:"is_commit_unknown"`
}

type foregroundPUTNodeIdentity struct {
	NodeID        string `json:"node_id"`
	CorePointer   string `json:"core_pointer"`
	ServerPointer string `json:"server_pointer"`
}

type foregroundPUTReceipt struct {
	RequestID string               `json:"request_id"`
	Found     bool                 `json:"found"`
	Status    types.MutationStatus `json:"status,omitempty"`
	Applied   bool                 `json:"applied"`
	Slot      uint64               `json:"slot"`
	Error     string               `json:"error,omitempty"`
}

type foregroundPUTBarrierState struct {
	Active                int                          `json:"active"`
	MaxActive             int                          `json:"max_active"`
	Calls                 int                          `json:"calls"`
	LastSlot              quepaxa.Slot                 `json:"last_slot"`
	LastDuration          time.Duration                `json:"last_duration_ns"`
	LastError             string                       `json:"last_error,omitempty"`
	LastSuccessfulThrough quepaxa.Slot                 `json:"last_successful_through"`
	FirstFailureSlot      quepaxa.Slot                 `json:"first_failure_slot"`
	FirstFailure          string                       `json:"first_failure,omitempty"`
	ActiveSlots           []foregroundPUTActiveBarrier `json:"active_slots,omitempty"`
	ActiveSlotsTruncated  bool                         `json:"active_slots_truncated"`
}

type foregroundPUTActiveBarrier struct {
	Slot quepaxa.Slot  `json:"slot"`
	Age  time.Duration `json:"age_ns"`
}

type foregroundPUTBarrierObservation struct {
	mu                   sync.Mutex
	enabled              bool
	nextID               uint64
	activeSlots          []foregroundPUTActiveBarrierEntry
	activeSlotsTruncated bool
	foregroundPUTBarrierState
}

type foregroundPUTActiveBarrierEntry struct {
	id      uint64
	slot    quepaxa.Slot
	started time.Time
}

func (o *foregroundPUTBarrierObservation) setEnabled(enabled bool) {
	o.mu.Lock()
	o.enabled = enabled
	o.mu.Unlock()
}

func (o *foregroundPUTBarrierObservation) run(ctx context.Context, slot quepaxa.Slot, syncThrough func(context.Context, quepaxa.Slot) error) error {
	started := time.Now()
	o.mu.Lock()
	if !o.enabled {
		o.mu.Unlock()
		return syncThrough(ctx, slot)
	}
	o.Active++
	o.Calls++
	o.nextID++
	id := o.nextID
	if len(o.activeSlots) < foregroundPUTDiagnosticLimit {
		o.activeSlots = append(o.activeSlots, foregroundPUTActiveBarrierEntry{id: id, slot: slot, started: started})
	} else {
		o.activeSlotsTruncated = true
	}
	if o.Active > o.MaxActive {
		o.MaxActive = o.Active
	}
	o.mu.Unlock()

	err := syncThrough(ctx, slot)
	o.mu.Lock()
	o.Active--
	for index, entry := range o.activeSlots {
		if entry.id == id {
			o.activeSlots = append(o.activeSlots[:index], o.activeSlots[index+1:]...)
			break
		}
	}
	o.LastSlot = slot
	o.LastDuration = time.Since(started)
	o.LastError = foregroundPUTShortError(err)
	if err == nil {
		if slot > o.LastSuccessfulThrough {
			o.LastSuccessfulThrough = slot
		}
	} else if o.FirstFailure == "" {
		o.FirstFailureSlot = slot
		o.FirstFailure = foregroundPUTShortError(err)
	}
	o.mu.Unlock()
	return err
}

func (o *foregroundPUTBarrierObservation) snapshot() foregroundPUTBarrierState {
	o.mu.Lock()
	defer o.mu.Unlock()
	state := o.foregroundPUTBarrierState
	state.ActiveSlotsTruncated = o.activeSlotsTruncated
	state.ActiveSlots = make([]foregroundPUTActiveBarrier, 0, len(o.activeSlots))
	for _, entry := range o.activeSlots {
		state.ActiveSlots = append(state.ActiveSlots, foregroundPUTActiveBarrier{Slot: entry.slot, Age: time.Since(entry.started)})
	}
	return state
}

type foregroundPUTFailureDiagnostic struct {
	mu                sync.Mutex
	records           []foregroundPUTFailure
	stack             string
	stackBytes        int
	stackTruncated    bool
	stackCaptured     bool
	stackCaptures     int
	barrierAtFirst    foregroundPUTBarrierState
	barrierAtFirstAt  time.Time
	barrier           *foregroundPUTBarrierObservation
	materials         *materializer.Materializer
	core              *quepaxa.Core
	archive           *recovery.Manager
	receipts          []foregroundPUTReceipt
	readbackError     string
	receiptObservedAt time.Time
	materialTip       uint64
	coreTip           quepaxa.Slot
	archiveTip        quepaxa.Slot
	configuredNodes   []foregroundPUTNodeIdentity
	firstErrorNodes   []foregroundPUTNodeIdentity
	nodeSnapshotAt    time.Time
}

func foregroundPUTConfiguredNodes(peers *foregroundAPIPeers) []foregroundPUTNodeIdentity {
	members := append([]quepaxa.Member(nil), peers.config.Members...)
	sort.Slice(members, func(i, j int) bool { return members[i].ID < members[j].ID })
	if len(members) > 3 {
		members = members[:3]
	}
	nodes := make([]foregroundPUTNodeIdentity, 0, len(members))
	for _, member := range members {
		nodes = append(nodes, foregroundPUTNodeIdentity{
			NodeID: string(member.ID), CorePointer: fmt.Sprintf("%p", peers.cores[member.ID]),
			ServerPointer: fmt.Sprintf("%p", peers.servers[member.ID]),
		})
	}
	return nodes
}

func foregroundPUTShortError(err error) string {
	if err == nil {
		return ""
	}
	const max = 512
	message := err.Error()
	if len(message) > max {
		return message[:max] + "…"
	}
	return message
}

func foregroundPUTErrorChain(err error) ([]foregroundPUTErrorNode, []foregroundPUTUnknown, bool) {
	type pending struct {
		err   error
		depth int
	}
	queue := []pending{{err: err}}
	nodes := make([]foregroundPUTErrorNode, 0, foregroundPUTErrorChainLimit)
	unknowns := make([]foregroundPUTUnknown, 0, 2)
	truncated := false
	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]
		if current.err == nil {
			continue
		}
		if len(nodes) == foregroundPUTErrorChainLimit {
			truncated = true
			break
		}
		nodes = append(nodes, foregroundPUTErrorNode{Depth: current.depth, Type: fmt.Sprintf("%T", current.err), Error: foregroundPUTShortError(current.err)})
		if unknown, ok := current.err.(*CommitUnknownError); ok {
			item := foregroundPUTUnknown{Slot: unknown.Slot, RetryThroughSlot: unknown.RetryThroughSlot, RequestID: unknown.RequestID}
			if unknown.Cause != nil {
				item.CauseType, item.Cause = fmt.Sprintf("%T", unknown.Cause), foregroundPUTShortError(unknown.Cause)
			}
			unknowns = append(unknowns, item)
		}
		if nested, ok := current.err.(interface{ Unwrap() []error }); ok {
			children := nested.Unwrap()
			if current.depth+1 >= foregroundPUTErrorChainLimit && len(children) != 0 {
				truncated = true
				continue
			}
			for _, child := range children {
				queue = append(queue, pending{err: child, depth: current.depth + 1})
			}
		} else if nested := errors.Unwrap(current.err); nested != nil {
			if current.depth+1 >= foregroundPUTErrorChainLimit {
				truncated = true
				continue
			}
			queue = append(queue, pending{err: nested, depth: current.depth + 1})
		}
	}
	return nodes, unknowns, truncated
}

func (d *foregroundPUTFailureDiagnostic) record(phase string, ctx context.Context, requestID string, started, completed, cutoff time.Time, err error) {
	d.recordWithStack(phase, ctx, requestID, started, completed, cutoff, err, runtime.Stack)
}

func (d *foregroundPUTFailureDiagnostic) recordWithStack(phase string, ctx context.Context, requestID string, started, completed, cutoff time.Time, err error, stack func([]byte, bool) int) {
	if err == nil {
		return
	}
	callDeadline, _ := ctx.Deadline()
	callContextErr := ctx.Err()
	errorChain, unknowns, chainTruncated := foregroundPUTErrorChain(err)
	cutoffKnown := !cutoff.IsZero()
	completionOffset := time.Duration(0)
	completedBeforeCutoff := false
	completionWindow := "outside-window"
	if cutoffKnown {
		completionOffset = completed.Sub(cutoff)
		completedBeforeCutoff = !completed.After(cutoff)
		completionWindow = "drain"
		if completedBeforeCutoff {
			completionWindow = "in-window"
		}
	}
	record := foregroundPUTFailure{
		Phase: phase, CompletionWindow: completionWindow, RequestID: requestID, ErrorType: fmt.Sprintf("%T", err), Error: foregroundPUTShortError(err),
		ErrorChain: errorChain, ErrorChainTruncated: chainTruncated, Unknowns: unknowns,
		Elapsed: completed.Sub(started), StartedAt: started, CompletedAt: completed,
		CutoffKnown: cutoffKnown, CompletionOffset: completionOffset, CompletedBeforeCutoff: completedBeforeCutoff,
		CallDeadline: callDeadline, CallContextErr: foregroundPUTShortError(callContextErr),
		IsDeadline: errors.Is(err, context.DeadlineExceeded), IsCanceled: errors.Is(err, context.Canceled),
		IsQuorumUnavailable: errors.Is(err, quepaxa.ErrQuorumUnavailable), IsCommitUnknown: errors.Is(err, ErrCommitUnknown),
	}
	d.mu.Lock()
	if len(d.records) < foregroundPUTDiagnosticLimit {
		d.records = append(d.records, record)
	}
	captureStack := !d.stackCaptured
	if captureStack {
		d.stackCaptured = true
		d.stackCaptures++
		d.firstErrorNodes = append([]foregroundPUTNodeIdentity(nil), d.configuredNodes...)
		d.nodeSnapshotAt = time.Now().UTC()
	}
	d.mu.Unlock()
	if !captureStack {
		return
	}

	barrierState := foregroundPUTBarrierState{}
	if d.barrier != nil {
		barrierState = d.barrier.snapshot()
	}
	barrierObservedAt := time.Now()
	buf := make([]byte, foregroundPUTStackLimit)
	n := stack(buf, true)
	d.mu.Lock()
	d.stack = string(buf[:n])
	d.stackBytes = n
	d.stackTruncated = n == len(buf)
	d.barrierAtFirst = barrierState
	d.barrierAtFirstAt = barrierObservedAt
	d.mu.Unlock()
}

func (d *foregroundPUTFailureDiagnostic) inspectAfterJoin() {
	d.mu.Lock()
	records := append([]foregroundPUTFailure(nil), d.records...)
	d.mu.Unlock()
	if d.materials == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	d.mu.Lock()
	d.receiptObservedAt = time.Now()
	d.mu.Unlock()
	for _, record := range records {
		if ctx.Err() != nil {
			d.mu.Lock()
			d.readbackError = ctx.Err().Error()
			d.mu.Unlock()
			break
		}
		receipt, found, err := d.materials.MutationReceipt(ctx, types.MutationKV, record.RequestID)
		observation := foregroundPUTReceipt{RequestID: record.RequestID, Found: found}
		if err != nil {
			observation.Error = foregroundPUTShortError(err)
		} else if found {
			observation.Status, observation.Applied, observation.Slot = receipt.Status, receipt.Applied, receipt.Slot
		}
		d.mu.Lock()
		d.receipts = append(d.receipts, observation)
		d.mu.Unlock()
	}
	if d.materials != nil {
		d.materialTip = d.materials.Tip()
	}
	if d.core != nil {
		d.coreTip = d.core.Tip()
	}
	if d.archive != nil {
		d.archiveTip = d.archive.Tip()
	}
}

func (d *foregroundPUTFailureDiagnostic) json() ([]byte, error) {
	barrierFinal := foregroundPUTBarrierState{}
	if d.barrier != nil {
		barrierFinal = d.barrier.snapshot()
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	return json.Marshal(struct {
		Purpose                string                      `json:"purpose"`
		TimestampSource        string                      `json:"timestamp_source"`
		Limit                  int                         `json:"record_limit"`
		Records                []foregroundPUTFailure      `json:"first_failures"`
		StackCaptured          bool                        `json:"stack_captured"`
		StackCaptures          int                         `json:"stack_capture_count"`
		StackBytes             int                         `json:"stack_bytes"`
		StackTruncated         bool                        `json:"stack_truncated"`
		Stack                  string                      `json:"all_goroutine_stack"`
		BarrierAtFirst         foregroundPUTBarrierState   `json:"barrier_at_first_error"`
		BarrierAtFirstAt       time.Time                   `json:"barrier_at_first_error_observed_at"`
		BarrierFinal           foregroundPUTBarrierState   `json:"barrier_after_join"`
		Receipts               []foregroundPUTReceipt      `json:"read_only_receipts"`
		ReceiptObserved        time.Time                   `json:"receipt_observed_at"`
		ReadbackError          string                      `json:"readback_error,omitempty"`
		MaterialTip            uint64                      `json:"materializer_tip"`
		CoreTip                quepaxa.Slot                `json:"core_tip"`
		ArchiveTip             quepaxa.Slot                `json:"archive_tip"`
		NodeIdentityCapturedAt time.Time                   `json:"first_error_node_identity_captured_at"`
		FirstErrorNodes        []foregroundPUTNodeIdentity `json:"first_error_nodes"`
		NodeDynamicState       string                      `json:"node_dynamic_state"`
	}{
		Purpose: "diagnostic-only; not performance or p99 qualification", TimestampSource: "time.Now wall clock at PUT closure; elapsed is monotonic duration",
		Limit: foregroundPUTDiagnosticLimit, Records: append([]foregroundPUTFailure(nil), d.records...),
		StackCaptured: d.stackCaptured, StackCaptures: d.stackCaptures, StackBytes: d.stackBytes, StackTruncated: d.stackTruncated, Stack: d.stack,
		BarrierAtFirst: d.barrierAtFirst, BarrierFinal: barrierFinal, Receipts: append([]foregroundPUTReceipt(nil), d.receipts...),
		BarrierAtFirstAt: d.barrierAtFirstAt,
		ReceiptObserved:  d.receiptObservedAt, ReadbackError: d.readbackError,
		MaterialTip: d.materialTip, CoreTip: d.coreTip, ArchiveTip: d.archiveTip,
		NodeIdentityCapturedAt: d.nodeSnapshotAt, FirstErrorNodes: append([]foregroundPUTNodeIdentity(nil), d.firstErrorNodes...),
		NodeDynamicState: "unavailable_no_nonblocking_getter",
	})
}

func logForegroundPUTDiagnostic(t testing.TB, diagnostic *foregroundPUTFailureDiagnostic) {
	t.Helper()
	if diagnostic == nil {
		return
	}
	diagnostic.mu.Lock()
	hasFailures := len(diagnostic.records) > 0
	diagnostic.mu.Unlock()
	if !hasFailures {
		return
	}
	result, err := diagnostic.json()
	if err != nil {
		t.Logf("FOREGROUND_API_PUT_DIAGNOSTIC marshal_error=%q", err.Error())
		return
	}
	t.Logf("FOREGROUND_API_PUT_DIAGNOSTIC %s", result)
}

func (a *apiLatencyAccumulator) add(elapsed time.Duration, err error, overlap, inWindow bool) {
	recordingStarted := time.Now()
	a.mu.Lock()
	a.started++
	if inWindow {
		a.all = append(a.all, elapsed)
	} else {
		a.drainAll = append(a.drainAll, elapsed)
	}
	if err == nil {
		if inWindow {
			a.success = append(a.success, elapsed)
		} else {
			a.drainSuccess = append(a.drainSuccess, elapsed)
		}
	} else {
		if inWindow {
			a.errors++
			if errors.Is(err, ErrNotReady) || errors.Is(err, quepaxa.ErrQuorumUnavailable) {
				a.rejections++
			}
			if errors.Is(err, context.DeadlineExceeded) || isForegroundTimeout(err) {
				a.timeouts++
			}
			if errors.Is(err, ErrCommitUnknown) {
				a.commitUnknown++
			}
		} else {
			a.drainErrors++
			if errors.Is(err, ErrNotReady) || errors.Is(err, quepaxa.ErrQuorumUnavailable) {
				a.drainRejections++
			}
			if errors.Is(err, context.DeadlineExceeded) || isForegroundTimeout(err) {
				a.drainTimeouts++
			}
			if errors.Is(err, ErrCommitUnknown) {
				a.drainCommitUnknown++
			}
		}
	}
	if overlap {
		if inWindow {
			a.overlapAll = append(a.overlapAll, elapsed)
			if err == nil {
				a.overlapSuccess = append(a.overlapSuccess, elapsed)
			}
		} else {
			a.drainOverlapAll = append(a.drainOverlapAll, elapsed)
			if err == nil {
				a.drainOverlapSuccess = append(a.drainOverlapSuccess, elapsed)
			}
		}
	}
	if inWindow {
		a.recording += time.Since(recordingStarted)
	} else {
		a.drainRecording += time.Since(recordingStarted)
	}
	a.mu.Unlock()
}

func isForegroundTimeout(err error) bool {
	var networkErr net.Error
	return errors.As(err, &networkErr) && networkErr.Timeout()
}

func (a *apiLatencyAccumulator) snapshot() apiLatencySample {
	a.mu.Lock()
	defer a.mu.Unlock()
	return apiLatencySample{
		all: append([]time.Duration(nil), a.all...), success: append([]time.Duration(nil), a.success...),
		overlapAll: append([]time.Duration(nil), a.overlapAll...), overlapSuccess: append([]time.Duration(nil), a.overlapSuccess...),
		drainAll: append([]time.Duration(nil), a.drainAll...), drainSuccess: append([]time.Duration(nil), a.drainSuccess...),
		drainOverlapAll: append([]time.Duration(nil), a.drainOverlapAll...), drainOverlapSuccess: append([]time.Duration(nil), a.drainOverlapSuccess...),
		started: a.started, errors: a.errors, rejections: a.rejections, timeouts: a.timeouts, commitUnknown: a.commitUnknown,
		drainErrors: a.drainErrors, drainRejections: a.drainRejections, drainTimeouts: a.drainTimeouts,
		drainCommitUnknown: a.drainCommitUnknown, recording: a.recording, drainRecording: a.drainRecording,
	}
}

func apiLatencyJSON(sample apiLatencySample, seconds float64) map[string]any {
	return map[string]any{
		"started_requests": sample.started,
		"requests":         len(sample.all), "successes": len(sample.success), "errors": sample.errors,
		"rejections": sample.rejections, "timeouts": sample.timeouts, "commit_unknown": sample.commitUnknown,
		"successful_ops_per_second":     float64(len(sample.success)) / seconds,
		"all_completion_ops_per_second": float64(len(sample.all)) / seconds,
		"sample_recording_ns_per_operation": func() float64 {
			if len(sample.all) == 0 {
				return 0
			}
			return float64(sample.recording) / float64(len(sample.all))
		}(),
		"drain_sample_recording_ns_per_operation": func() float64 {
			if len(sample.drainAll) == 0 {
				return 0
			}
			return float64(sample.drainRecording) / float64(len(sample.drainAll))
		}(),
		"success_p99_ms": nearestRankDurationP99(sample.success), "all_completion_p99_ms": nearestRankDurationP99(sample.all),
		"maintenance_overlap_requests":                    len(sample.overlapAll),
		"maintenance_overlap_success_p99_ms":              nearestRankDurationP99(sample.overlapSuccess),
		"maintenance_overlap_all_completion_p99_ms":       nearestRankDurationP99(sample.overlapAll),
		"drain_completions":                               len(sample.drainAll),
		"drain_successes":                                 len(sample.drainSuccess),
		"drain_errors":                                    sample.drainErrors,
		"drain_rejections":                                sample.drainRejections,
		"drain_timeouts":                                  sample.drainTimeouts,
		"drain_commit_unknown":                            sample.drainCommitUnknown,
		"drain_all_completion_p99_ms":                     nearestRankDurationP99(sample.drainAll),
		"drain_success_p99_ms":                            nearestRankDurationP99(sample.drainSuccess),
		"drain_maintenance_overlap_requests":              len(sample.drainOverlapAll),
		"drain_maintenance_overlap_success_p99_ms":        nearestRankDurationP99(sample.drainOverlapSuccess),
		"drain_maintenance_overlap_all_completion_p99_ms": nearestRankDurationP99(sample.drainOverlapAll),
		"all_cohort_errors":                               sample.errors + sample.drainErrors,
		"all_cohort_timeouts":                             sample.timeouts + sample.drainTimeouts,
		"all_cohort_commit_unknown":                       sample.commitUnknown + sample.drainCommitUnknown,
		"all_cohort_completions":                          len(sample.all) + len(sample.drainAll),
	}
}

type foregroundAPIWindowGate struct {
	mu              sync.Mutex
	deadline        time.Time
	closed          bool
	active          int
	pendingAtCutoff int
}

func (g *foregroundAPIWindowGate) beginAt(now time.Time) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.beginLocked(now)
}

func (g *foregroundAPIWindowGate) begin() (time.Time, bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	started := time.Now()
	return started, g.beginLocked(started)
}

func (g *foregroundAPIWindowGate) beginLocked(now time.Time) bool {
	if g.closed || !now.Before(g.deadline) {
		if !g.closed {
			g.closed = true
			g.pendingAtCutoff = g.active
		}
		return false
	}
	g.active++
	return true
}

func (g *foregroundAPIWindowGate) close() {
	g.mu.Lock()
	defer g.mu.Unlock()
	if !g.closed {
		g.closed = true
		g.pendingAtCutoff = g.active
	}
}

func (g *foregroundAPIWindowGate) finish() {
	g.mu.Lock()
	g.active--
	g.mu.Unlock()
}

func (g *foregroundAPIWindowGate) snapshot() (pendingAtCutoff, outstanding int) {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.pendingAtCutoff, g.active
}

func runForegroundAPICall(parent context.Context, gate *foregroundAPIWindowGate, maxDuration time.Duration, call func(context.Context) error, onError func(context.Context, time.Time, time.Time, error)) (started, completed time.Time, err error, admitted bool) {
	if parent.Err() != nil {
		return time.Time{}, time.Time{}, nil, false
	}
	if started, admitted = gate.begin(); !admitted {
		return time.Time{}, time.Time{}, nil, false
	}
	defer gate.finish()
	callCtx, cancel := context.WithTimeout(parent, maxDuration)
	defer cancel()
	err = call(callCtx)
	completed = time.Now()
	if err != nil && onError != nil {
		onError(callCtx, started, completed, err)
	}
	return started, completed, err, true
}

func nearestRankDurationP99(values []time.Duration) float64 {
	if len(values) == 0 {
		return 0
	}
	ordered := append([]time.Duration(nil), values...)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i] < ordered[j] })
	return float64(ordered[(99*len(ordered)+99)/100-1]) / float64(time.Millisecond)
}

func stopForegroundMaintenance(cancel context.CancelFunc, workers *sync.WaitGroup) {
	cancel()
	workers.Wait()
}

func foregroundMaintenanceFailure(scenario, stage string, err error, ctx context.Context, elapsed time.Duration) (string, bool) {
	if err == nil || errors.Is(err, context.Canceled) {
		return "", false
	}
	if prefix, _, ok := strings.Cut(err.Error(), ": "); ok {
		switch prefix {
		case "checkpoint-pre-sync", "checkpoint-publication", "certified-seal-read", "certified-seal-validation", "fresh-current-readback", "fresh-current-comparison":
			stage = prefix
		}
	}
	return fmt.Sprintf("scenario=%s stage=%s error=%q error_is_canceled=%t error_is_deadline_exceeded=%t context_error=%v elapsed=%s", scenario, stage, err, errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded), ctx.Err(), elapsed), true
}

func TestForegroundMaintenanceFailureKeepsCauseAndFiltersWrappedCancellation(t *testing.T) {
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if observation, counted := foregroundMaintenanceFailure("checkpoint-active", "checkpoint-publication", fmt.Errorf("publication: %w", context.Canceled), canceled, time.Second); counted || observation != "" {
		t.Fatalf("wrapped cancellation observation=%q counted=%t; want filtered", observation, counted)
	}

	deadlineErr := fmt.Errorf("archive sync: %w", context.DeadlineExceeded)
	observation, counted := foregroundMaintenanceFailure("checkpoint-active", "checkpoint-pre-sync", deadlineErr, context.Background(), 2*time.Second)
	if !counted || !strings.Contains(observation, "scenario=checkpoint-active") || !strings.Contains(observation, "stage=checkpoint-pre-sync") || !strings.Contains(observation, "error_is_deadline_exceeded=true") || !strings.Contains(observation, "error=\"archive sync: context deadline exceeded\"") {
		t.Fatalf("deadline failure observation=%q counted=%t", observation, counted)
	}

	nonContextErr := errors.New("publication mismatch")
	observation, counted = foregroundMaintenanceFailure("checkpoint-active", "fresh-current-readback", nonContextErr, canceled, 3*time.Second)
	if !counted || !strings.Contains(observation, "context_error=context canceled") || !strings.Contains(observation, "error_is_canceled=false") {
		t.Fatalf("non-context failure observation=%q counted=%t", observation, counted)
	}
}

func TestStopForegroundMaintenanceCancelsBlockedOperationBeforeJoin(t *testing.T) {
	operationCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var workers sync.WaitGroup
	workers.Add(1)
	started := make(chan struct{})
	go func() {
		defer workers.Done()
		close(started)
		<-operationCtx.Done()
	}()
	<-started
	stopped := make(chan struct{})
	go func() {
		stopForegroundMaintenance(cancel, &workers)
		close(stopped)
	}()
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("maintenance join did not cancel the blocked operation")
	}
}

func foregroundAPISeedDeadline(parentDeadline time.Time, hasParentDeadline bool, runnerDeadline time.Time) (time.Time, bool) {
	if runnerDeadline.IsZero() {
		return time.Time{}, false
	}
	seedDeadline := runnerDeadline.Add(-(foregroundAPIWindowTotal + foregroundAPIWindowCleanupReserve))
	if hasParentDeadline && !seedDeadline.Before(parentDeadline) {
		return parentDeadline, false
	}
	return seedDeadline, true
}

func withForegroundAPISeedBudget(parent context.Context, runnerDeadline time.Time) (context.Context, context.CancelFunc, time.Time, bool) {
	parentDeadline, hasParentDeadline := parent.Deadline()
	seedDeadline, runnerBound := foregroundAPISeedDeadline(parentDeadline, hasParentDeadline, runnerDeadline)
	if seedDeadline.IsZero() {
		ctx, cancel := context.WithCancel(parent)
		return ctx, cancel, time.Time{}, false
	}
	ctx, cancel := context.WithDeadline(parent, seedDeadline)
	return ctx, cancel, seedDeadline, runnerBound
}

func foregroundAPISeedResult(err error, ctx context.Context, budgetDeadline time.Time, runnerBound bool) error {
	contextErr := ctx.Err()
	if runnerBound && !budgetDeadline.IsZero() && !time.Now().Before(budgetDeadline) {
		return errors.Join(errForegroundAPISeedBudgetExpired, err, context.DeadlineExceeded)
	}
	return errors.Join(err, contextErr)
}

func seedForegroundAPIKeys(ctx context.Context, put func(context.Context, KVMutationRequest) error, value []byte, clientID int64, budgetDeadline time.Time, runnerBound bool, progress func(int)) error {
	if err := foregroundAPISeedResult(nil, ctx, budgetDeadline, runnerBound); err != nil {
		return err
	}
	seedCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	jobs := make(chan int)
	var workers sync.WaitGroup
	var completed atomic.Uint64
	var errMu sync.Mutex
	var seedErr error
	workers.Add(foregroundAPIWorkers)
	for range foregroundAPIWorkers {
		go func() {
			defer workers.Done()
			for {
				select {
				case <-seedCtx.Done():
					return
				case index, ok := <-jobs:
					if !ok {
						return
					}
					requestID := fmt.Sprintf("foreground-seed-%d-%d", clientID, index)
					key := fmt.Sprintf("key-%04d", index)
					if err := put(seedCtx, KVMutationRequest{RequestID: requestID, Key: key, Value: value}); err != nil {
						errMu.Lock()
						seedErr = errors.Join(seedErr, fmt.Errorf("seed key %d: %w", index, err))
						errMu.Unlock()
						cancel()
						return
					}
					count := int(completed.Add(1))
					if progress != nil && (count%foregroundAPISeedLogEvery == 0 || count == foregroundAPIKeys) {
						progress(count)
					}
				}
			}
		}()
	}

dispatch:
	for index := range foregroundAPIKeys {
		select {
		case jobs <- index:
		case <-seedCtx.Done():
			break dispatch
		}
	}
	close(jobs)
	workers.Wait()

	errMu.Lock()
	defer errMu.Unlock()
	return foregroundAPISeedResult(seedErr, ctx, budgetDeadline, runnerBound)
}

func foregroundAPIWindowBudgetAvailable(deadline, now time.Time) bool {
	if deadline.IsZero() {
		return true
	}
	return deadline.Sub(now) >= foregroundAPIWindowTotal+foregroundAPIWindowCleanupReserve
}

func requireForegroundAPIWindowBudget(t testing.TB, deadline time.Time, phase string) {
	t.Helper()
	now := time.Now()
	if !foregroundAPIWindowBudgetAvailable(deadline, now) {
		t.Fatalf("insufficient test deadline at %s before fixed foreground windows: remaining=%s required=%s (windows=%s cleanup_reserve=%s)", phase, deadline.Sub(now), foregroundAPIWindowTotal+foregroundAPIWindowCleanupReserve, foregroundAPIWindowTotal, foregroundAPIWindowCleanupReserve)
	}
}

type foregroundAPISeedRecorder struct {
	mu          sync.Mutex
	cond        *sync.Cond
	requests    map[string]KVMutationRequest
	failRequest string
	failure     error
	barrier     int
	started     int
	active      int
	maximum     int
}

func newForegroundAPISeedRecorder(barrier int) *foregroundAPISeedRecorder {
	recorder := &foregroundAPISeedRecorder{requests: make(map[string]KVMutationRequest), barrier: barrier}
	recorder.cond = sync.NewCond(&recorder.mu)
	return recorder
}

func (recorder *foregroundAPISeedRecorder) put(ctx context.Context, request KVMutationRequest) error {
	recorder.mu.Lock()
	recorder.active++
	if recorder.active > recorder.maximum {
		recorder.maximum = recorder.active
	}
	recorder.started++
	if recorder.barrier > 0 && recorder.started <= recorder.barrier && recorder.started < recorder.barrier {
		for recorder.started < recorder.barrier {
			recorder.cond.Wait()
		}
	} else if recorder.barrier > 0 && recorder.started == recorder.barrier {
		recorder.cond.Broadcast()
	}
	request.Value = append([]byte(nil), request.Value...)
	recorder.requests[request.RequestID] = request
	err := recorder.failure
	if request.RequestID != recorder.failRequest {
		err = nil
	}
	if err == nil {
		err = ctx.Err()
	}
	recorder.active--
	recorder.mu.Unlock()
	return err
}

func TestForegroundAPISeedPreservesExactRequestsWithBoundedConcurrency(t *testing.T) {
	const clientID = int64(42)
	value := make([]byte, foregroundAPIValue)
	for index := range value {
		value[index] = byte((index*31 + 7) % 251)
	}
	recorder := newForegroundAPISeedRecorder(foregroundAPIWorkers)
	var progressMu sync.Mutex
	progress := make(map[int]bool)
	if err := seedForegroundAPIKeys(context.Background(), recorder.put, value, clientID, time.Time{}, false, func(count int) {
		progressMu.Lock()
		progress[count] = true
		progressMu.Unlock()
	}); err != nil {
		t.Fatalf("seedForegroundAPIKeys: %v", err)
	}

	recorder.mu.Lock()
	requestCount, maximum, requests := len(recorder.requests), recorder.maximum, recorder.requests
	recorder.mu.Unlock()
	if requestCount != foregroundAPIKeys {
		t.Fatalf("seed request count=%d, want %d", requestCount, foregroundAPIKeys)
	}
	if maximum != foregroundAPIWorkers {
		t.Fatalf("maximum concurrent seed calls=%d, want exactly %d from the barrier", maximum, foregroundAPIWorkers)
	}
	for index := range foregroundAPIKeys {
		requestID := fmt.Sprintf("foreground-seed-%d-%d", clientID, index)
		request, ok := requests[requestID]
		if !ok {
			t.Fatalf("missing seed request %q", requestID)
		}
		if request.Key != fmt.Sprintf("key-%04d", index) || !bytes.Equal(request.Value, value) {
			t.Fatalf("seed request %q has key=%q value_bytes=%d", requestID, request.Key, len(request.Value))
		}
	}
	progressMu.Lock()
	defer progressMu.Unlock()
	for count := foregroundAPISeedLogEvery; count <= foregroundAPIKeys; count += foregroundAPISeedLogEvery {
		if !progress[count] {
			t.Errorf("missing seed progress milestone %d", count)
		}
	}
}

func TestForegroundAPISeedFailureCancelsAndJoinsWorkers(t *testing.T) {
	wantErr := errors.New("seed failure")
	const clientID = int64(43)
	recorder := newForegroundAPISeedRecorder(foregroundAPIWorkers)
	recorder.failRequest = fmt.Sprintf("foreground-seed-%d-%d", clientID, 17)
	recorder.failure = wantErr
	err := seedForegroundAPIKeys(context.Background(), recorder.put, make([]byte, foregroundAPIValue), clientID, time.Time{}, false, nil)
	if !errors.Is(err, wantErr) {
		t.Fatalf("seed error=%v, want wrapped %v", err, wantErr)
	}
	recorder.mu.Lock()
	defer recorder.mu.Unlock()
	if recorder.active != 0 {
		t.Fatalf("seed helper returned with %d active workers", recorder.active)
	}
	if len(recorder.requests) == 0 || len(recorder.requests) > foregroundAPIKeys {
		t.Fatalf("failure path attempted %d unique requests", len(recorder.requests))
	}
}

func TestForegroundAPISeedParentCancellationJoinsWorkers(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	started := make(chan struct{}, foregroundAPIWorkers)
	var active atomic.Int64
	put := func(ctx context.Context, _ KVMutationRequest) error {
		active.Add(1)
		started <- struct{}{}
		<-ctx.Done()
		active.Add(-1)
		return ctx.Err()
	}
	done := make(chan error, 1)
	go func() {
		done <- seedForegroundAPIKeys(ctx, put, make([]byte, foregroundAPIValue), 44, time.Time{}, false, nil)
	}()
	for range foregroundAPIWorkers {
		<-started
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("seed error=%v, want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("seed helper did not join workers after parent cancellation")
	}
	if got := active.Load(); got != 0 {
		t.Fatalf("seed helper returned with %d active KVPut calls", got)
	}
}

func TestForegroundAPIWindowBudgetRequiresBothFixedWindowsAndCleanupReserve(t *testing.T) {
	deadline := time.Unix(1000, 0)
	minimumStart := deadline.Add(-(foregroundAPIWindowTotal + foregroundAPIWindowCleanupReserve))
	for _, test := range []struct {
		name string
		now  time.Time
		want bool
	}{
		{name: "exact minimum", now: minimumStart, want: true},
		{name: "one nanosecond short", now: minimumStart.Add(time.Nanosecond), want: false},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := foregroundAPIWindowBudgetAvailable(deadline, test.now); got != test.want {
				t.Fatalf("budget available=%t, want %t", got, test.want)
			}
		})
	}
	if !foregroundAPIWindowBudgetAvailable(time.Time{}, deadline) {
		t.Fatal("missing test deadline must leave the fixed windows runnable")
	}
}

func TestForegroundPUTDiagnosticCapturesConfiguredNodeIdentityOnce(t *testing.T) {
	peers := newForegroundAPIPeers(t, t.TempDir())
	configured := foregroundPUTConfiguredNodes(peers)
	diagnostic := &foregroundPUTFailureDiagnostic{configuredNodes: configured}
	started := time.Now()
	stackCalls := 0
	stack := func(buf []byte, all bool) int {
		stackCalls++
		if !all {
			t.Error("stack capture must include all goroutines")
		}
		return copy(buf, "first error stack")
	}
	diagnostic.recordWithStack("measurement", context.Background(), "no-error", started, started, time.Time{}, nil, stack)
	if diagnostic.stackCaptured || len(diagnostic.firstErrorNodes) != 0 || diagnostic.nodeSnapshotAt != (time.Time{}) {
		t.Fatal("nil error captured node identity")
	}
	diagnostic.recordWithStack("measurement", context.Background(), "first", started, time.Now(), time.Time{}, errors.New("first PUT failure"), stack)
	firstCapturedAt := diagnostic.nodeSnapshotAt
	configured[0].NodeID = "mutated-after-capture"
	diagnostic.recordWithStack("drain", context.Background(), "second", started, time.Now(), time.Time{}, errors.New("second PUT failure"), stack)
	if !diagnostic.stackCaptured || diagnostic.stackCaptures != 1 || stackCalls != 1 || diagnostic.nodeSnapshotAt != firstCapturedAt {
		t.Fatalf("first-error capture changed on subsequent failures: stack=%t captures=%d stack_calls=%d at=%s", diagnostic.stackCaptured, diagnostic.stackCaptures, stackCalls, diagnostic.nodeSnapshotAt)
	}

	encoded, err := diagnostic.json()
	if err != nil {
		t.Fatal(err)
	}
	var got struct {
		CapturedAt time.Time                   `json:"first_error_node_identity_captured_at"`
		Nodes      []foregroundPUTNodeIdentity `json:"first_error_nodes"`
		State      string                      `json:"node_dynamic_state"`
	}
	if err := json.Unmarshal(encoded, &got); err != nil {
		t.Fatal(err)
	}
	if got.CapturedAt.IsZero() || len(got.Nodes) != 3 {
		t.Fatalf("node identity JSON capture time=%s nodes=%+v", got.CapturedAt, got.Nodes)
	}
	for i, id := range []quepaxa.NodeID{"n1", "n2", "n3"} {
		if got.Nodes[i].NodeID != string(id) ||
			got.Nodes[i].CorePointer != fmt.Sprintf("%p", peers.cores[id]) ||
			got.Nodes[i].ServerPointer != fmt.Sprintf("%p", peers.servers[id]) {
			t.Fatalf("node identity %d was not ordered and copied immutably: %+v", i, got.Nodes[i])
		}
	}
	if got.State != "unavailable_no_nonblocking_getter" {
		t.Fatalf("dynamic node state must remain unavailable: %q", got.State)
	}
}

func TestForegroundPUTFailureDiagnosticIsBoundedAndReadOnly(t *testing.T) {
	material, err := materializer.Open(filepath.Join(t.TempDir(), "diagnostic.sqlite"), 1)
	if err != nil {
		t.Fatal(err)
	}
	defer material.Close()
	barrier := &foregroundPUTBarrierObservation{}
	barrier.setEnabled(true)
	diagnostic := &foregroundPUTFailureDiagnostic{barrier: barrier, materials: material}
	stats := &apiLatencyAccumulator{}
	cutoff := time.Now().Add(time.Second)
	gate := &foregroundAPIWindowGate{deadline: cutoff}
	barrierEntered, releaseBarrier := make(chan struct{}), make(chan struct{})
	barrierDone := make(chan struct{})
	go func() {
		defer close(barrierDone)
		if err := barrier.run(context.Background(), 7, func(context.Context, quepaxa.Slot) error {
			close(barrierEntered)
			<-releaseBarrier
			return nil
		}); err != nil {
			t.Errorf("diagnostic barrier: %v", err)
		}
	}()
	<-barrierEntered
	want := &CommitUnknownError{RequestID: "first-failure", Cause: context.DeadlineExceeded}
	var observedContext context.Context
	started, completed, err, admitted := runForegroundAPICall(context.Background(), gate, time.Second, func(ctx context.Context) error {
		observedContext = ctx
		return want
	}, func(ctx context.Context, start, end time.Time, callErr error) {
		diagnostic.record("measurement", ctx, "first-failure", start, end, cutoff, callErr)
		if ctx.Err() != nil {
			t.Errorf("call context was canceled before diagnostics: %v", ctx.Err())
		}
	})
	if !admitted || !errors.Is(err, ErrCommitUnknown) || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("first call admitted=%t err=%v", admitted, err)
	}
	if observedContext.Err() != context.Canceled {
		t.Fatalf("helper did not cancel call context after diagnostic: %v", observedContext.Err())
	}
	close(releaseBarrier)
	<-barrierDone
	stats.add(completed.Sub(started), err, false, true)

	const concurrentFailures = foregroundPUTDiagnosticLimit + 5
	var workers sync.WaitGroup
	workers.Add(concurrentFailures)
	for index := range concurrentFailures {
		go func(index int) {
			defer workers.Done()
			requestID := fmt.Sprintf("failure-%d", index)
			failure := fmt.Errorf("%w: %s", ErrCommitUnknown, requestID)
			start, end := time.Now(), time.Now()
			diagnostic.record("drain", context.Background(), requestID, start, end, cutoff, failure)
			stats.add(end.Sub(start), failure, false, false)
		}(index)
	}
	workers.Wait()
	if got := material.Tip(); got != 0 {
		t.Fatalf("diagnostic test materializer tip before readback=%d, want 0", got)
	}
	diagnostic.inspectAfterJoin()
	if got := material.Tip(); got != 0 {
		t.Fatalf("receipt inspection mutated materializer tip=%d", got)
	}

	diagnostic.mu.Lock()
	records := append([]foregroundPUTFailure(nil), diagnostic.records...)
	stackCaptures, stackBytes, stackTruncated, stack := diagnostic.stackCaptures, diagnostic.stackBytes, diagnostic.stackTruncated, diagnostic.stack
	barrierAtFirst, receiptObservedAt := diagnostic.barrierAtFirst, diagnostic.receiptObservedAt
	receipts := append([]foregroundPUTReceipt(nil), diagnostic.receipts...)
	diagnostic.mu.Unlock()
	if len(records) != foregroundPUTDiagnosticLimit {
		t.Fatalf("diagnostic retained %d failures, want cap %d", len(records), foregroundPUTDiagnosticLimit)
	}
	if records[0].RequestID != "first-failure" || records[0].CallContextErr != "" || len(records[0].ErrorChain) < 2 || len(records[0].Unknowns) != 1 || records[0].Unknowns[0].Slot != 0 {
		t.Fatalf("first failure provenance=%+v", records[0])
	}
	if records[0].Phase != "measurement" || records[0].CompletionWindow != "in-window" || records[0].CallDeadline.IsZero() || records[0].StartedAt.IsZero() || records[0].CompletedAt.IsZero() {
		t.Fatalf("first failure time/phase provenance=%+v", records[0])
	}
	if stackCaptures != 1 || stackBytes <= 0 || stackBytes > foregroundPUTStackLimit || stack == "" {
		t.Fatalf("stack captures=%d bytes=%d length=%d", stackCaptures, stackBytes, len(stack))
	}
	if stackTruncated != (stackBytes == foregroundPUTStackLimit) {
		t.Fatalf("stack truncation flag=%t bytes=%d", stackTruncated, stackBytes)
	}
	if barrierAtFirst.Active != 1 || len(barrierAtFirst.ActiveSlots) != 1 || barrierAtFirst.ActiveSlots[0].Slot != 7 {
		t.Fatalf("first-error active barrier snapshot=%+v", barrierAtFirst)
	}
	if len(receipts) != foregroundPUTDiagnosticLimit {
		t.Fatalf("post-join receipt checks=%d, want at most %d", len(receipts), foregroundPUTDiagnosticLimit)
	}
	if receiptObservedAt.IsZero() {
		t.Fatal("read-only receipt checks have no observation timestamp")
	}
	for _, receipt := range receipts {
		if receipt.Found || receipt.Error != "" {
			t.Fatalf("unexpected read-only receipt observation: %+v", receipt)
		}
	}
	sample := stats.snapshot()
	if sample.started != concurrentFailures+1 || sample.errors != 1 || sample.commitUnknown != 1 || sample.drainErrors != concurrentFailures || sample.drainCommitUnknown != concurrentFailures {
		t.Fatalf("diagnostic changed API outcome counters: %+v", sample)
	}
}

func TestForegroundPUTBarrierSnapshotPrecedesStackCapture(t *testing.T) {
	barrier := &foregroundPUTBarrierObservation{}
	barrier.setEnabled(true)
	diagnostic := &foregroundPUTFailureDiagnostic{barrier: barrier}
	entered, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	go func() {
		defer close(done)
		if err := barrier.run(context.Background(), 19, func(context.Context, quepaxa.Slot) error {
			close(entered)
			<-release
			return nil
		}); err != nil {
			t.Errorf("barrier run: %v", err)
		}
	}()
	<-entered
	started := time.Now()
	diagnostic.recordWithStack("measurement", context.Background(), "interleave", started, started, time.Time{}, errors.New("trigger"), func(buf []byte, all bool) int {
		if !all {
			t.Error("stack capture must include all goroutines")
		}
		close(release)
		<-done
		return copy(buf, "interleaved stack")
	})
	diagnostic.mu.Lock()
	first, capturedAt := diagnostic.barrierAtFirst, diagnostic.barrierAtFirstAt
	diagnostic.mu.Unlock()
	final := barrier.snapshot()
	if first.Active != 1 || len(first.ActiveSlots) != 1 || first.ActiveSlots[0].Slot != 19 {
		t.Fatalf("first snapshot lost active barrier during stack capture: %+v", first)
	}
	if capturedAt.IsZero() {
		t.Fatal("first barrier snapshot has no observation timestamp")
	}
	if final.Active != 0 || len(final.ActiveSlots) != 0 {
		t.Fatalf("barrier should be inactive after injected stack interleaving: %+v", final)
	}
}

func TestForegroundAPIWindowCutoffDrainsAdmittedCalls(t *testing.T) {
	for _, workers := range []int{1, foregroundAPIWorkers} {
		t.Run(fmt.Sprintf("workers-%d", workers), func(t *testing.T) {
			deadline := time.Now().Add(40 * time.Millisecond)
			gate := &foregroundAPIWindowGate{deadline: deadline}
			release := make(chan struct{})
			started := make(chan struct{}, workers)
			type result struct {
				start, end time.Time
				err        error
				admitted   bool
			}
			results := make(chan result, workers)
			var calls sync.WaitGroup
			calls.Add(workers)
			for range workers {
				go func() {
					defer calls.Done()
					start, end, err, admitted := runForegroundAPICall(context.Background(), gate, time.Second, func(ctx context.Context) error {
						started <- struct{}{}
						select {
						case <-release:
							return nil
						case <-ctx.Done():
							return ctx.Err()
						}
					}, nil)
					results <- result{start: start, end: end, err: err, admitted: admitted}
				}()
			}
			for range workers {
				select {
				case <-started:
				case <-time.After(time.Second):
					t.Fatal("call did not start")
				}
			}
			timer := time.NewTimer(time.Until(deadline))
			<-timer.C
			gate.close()
			pending, _ := gate.snapshot()
			if pending != workers {
				t.Fatalf("pending at cutoff=%d, want %d", pending, workers)
			}
			if gate.beginAt(deadline) {
				t.Fatal("call admitted at exact cutoff")
			}
			close(release)
			calls.Wait()
			close(results)
			stats := &apiLatencyAccumulator{}
			for result := range results {
				if !result.admitted || result.err != nil {
					t.Fatalf("call admitted=%t err=%v, want successful admitted call", result.admitted, result.err)
				}
				if !result.start.Before(deadline) || !result.end.After(deadline) {
					t.Fatalf("call interval start=%s end=%s deadline=%s did not straddle cutoff", result.start, result.end, deadline)
				}
				stats.add(result.end.Sub(result.start), result.err, false, !result.end.After(deadline))
			}
			sample := stats.snapshot()
			if sample.started != workers || len(sample.all) != 0 || len(sample.success) != 0 || len(sample.drainAll) != workers || len(sample.drainSuccess) != workers || sample.drainErrors != 0 {
				t.Fatalf("window/drain accounting=%+v, want %d successful drain completions", sample, workers)
			}
		})
	}

	gate := &foregroundAPIWindowGate{deadline: time.Now().Add(time.Second)}
	if !gate.beginAt(gate.deadline.Add(-time.Nanosecond)) || gate.beginAt(gate.deadline) {
		t.Fatal("admission gate did not accept before and reject at the exact cutoff")
	}
	gate.finish()
	gate.close()
}

func TestForegroundAPICallTimeoutAndParentCancellationRemainErrors(t *testing.T) {
	t.Run("operation timeout before cutoff", func(t *testing.T) {
		deadline := time.Now().Add(time.Second)
		gate := &foregroundAPIWindowGate{deadline: deadline}
		started, completed, err, admitted := runForegroundAPICall(context.Background(), gate, 10*time.Millisecond, func(ctx context.Context) error {
			<-ctx.Done()
			return ctx.Err()
		}, nil)
		gate.close()
		if !admitted || !errors.Is(err, context.DeadlineExceeded) || !completed.Before(deadline) {
			t.Fatalf("started=%s completed=%s deadline=%s admitted=%t err=%v", started, completed, deadline, admitted, err)
		}
		stats := &apiLatencyAccumulator{}
		stats.add(completed.Sub(started), err, false, true)
		sample := stats.snapshot()
		if sample.errors != 1 || sample.timeouts != 1 || sample.drainErrors != 0 {
			t.Fatalf("in-window timeout accounting=%+v", sample)
		}
		if sample.started != 1 || len(sample.all) != 1 || len(sample.drainAll) != 0 {
			t.Fatalf("in-window completion accounting=%+v", sample)
		}
	})
	t.Run("drain commit-unknown remains explicit", func(t *testing.T) {
		stats := &apiLatencyAccumulator{}
		unknown := fmt.Errorf("mutation result: %w: %w", ErrCommitUnknown, context.DeadlineExceeded)
		stats.add(time.Millisecond, unknown, false, false)
		sample := stats.snapshot()
		if sample.started != 1 || sample.errors != 0 || sample.drainErrors != 1 || sample.drainTimeouts != 1 || sample.drainCommitUnknown != 1 {
			t.Fatalf("drain unknown accounting=%+v", sample)
		}
	})

	t.Run("parent cancellation", func(t *testing.T) {
		parent, cancel := context.WithCancel(context.Background())
		gate := &foregroundAPIWindowGate{deadline: time.Now().Add(time.Second)}
		startedCall := make(chan struct{})
		result := make(chan error, 1)
		go func() {
			_, _, err, admitted := runForegroundAPICall(parent, gate, time.Second, func(ctx context.Context) error {
				close(startedCall)
				<-ctx.Done()
				return ctx.Err()
			}, nil)
			if !admitted && err == nil {
				result <- errors.New("call was not admitted")
				return
			}
			result <- err
		}()
		<-startedCall
		cancel()
		if err := <-result; !errors.Is(err, context.Canceled) {
			t.Fatalf("parent cancellation error=%v", err)
		}
		gate.close()
		if _, outstanding := gate.snapshot(); outstanding != 0 {
			t.Fatalf("parent cancellation left %d calls outstanding", outstanding)
		}
	})
	t.Run("operation deadline during drain remains error", func(t *testing.T) {
		deadline := time.Now().Add(10 * time.Millisecond)
		gate := &foregroundAPIWindowGate{deadline: deadline}
		startedCall := make(chan struct{})
		result := make(chan struct {
			started, completed time.Time
			err                error
			admitted           bool
		}, 1)
		go func() {
			started, completed, err, admitted := runForegroundAPICall(context.Background(), gate, 40*time.Millisecond, func(ctx context.Context) error {
				close(startedCall)
				<-ctx.Done()
				return ctx.Err()
			}, nil)
			result <- struct {
				started, completed time.Time
				err                error
				admitted           bool
			}{started: started, completed: completed, err: err, admitted: admitted}
		}()
		<-startedCall
		timer := time.NewTimer(time.Until(deadline))
		<-timer.C
		gate.close()
		outcome := <-result
		if !outcome.admitted || !errors.Is(outcome.err, context.DeadlineExceeded) || !outcome.completed.After(deadline) {
			t.Fatalf("drain operation result=%+v deadline=%s", outcome, deadline)
		}
		stats := &apiLatencyAccumulator{}
		stats.add(outcome.completed.Sub(outcome.started), outcome.err, false, false)
		sample := stats.snapshot()
		if sample.errors != 0 || sample.timeouts != 0 || sample.drainErrors != 1 || sample.drainTimeouts != 1 {
			t.Fatalf("drain timeout accounting=%+v", sample)
		}
	})
}

func TestForegroundAPIRecordingCostUsesMatchingCompletionCohort(t *testing.T) {
	stats := &apiLatencyAccumulator{}
	stats.add(time.Millisecond, nil, false, true)
	stats.add(2*time.Millisecond, context.DeadlineExceeded, false, false)
	sample := stats.snapshot()
	result := apiLatencyJSON(sample, 120)

	if sample.started != 2 || len(sample.all) != 1 || len(sample.drainAll) != 1 {
		t.Fatalf("cohort counts=%+v, want 2 started, 1 in-window and 1 drain completion", sample)
	}
	if got := result["started_requests"]; got != 2 {
		t.Fatalf("started_requests=%v, want all 2 admitted calls", got)
	}
	if got := result["requests"]; got != 1 {
		t.Fatalf("requests=%v, want 1 in-window completion", got)
	}
	if got, want := result["sample_recording_ns_per_operation"], float64(sample.recording)/float64(len(sample.all)); got != want {
		t.Fatalf("in-window recording cost=%v, want %v", got, want)
	}
	if got, want := result["drain_sample_recording_ns_per_operation"], float64(sample.drainRecording)/float64(len(sample.drainAll)); got != want {
		t.Fatalf("drain recording cost=%v, want %v", got, want)
	}
	if sample.recording <= 0 || sample.drainRecording <= 0 {
		t.Fatalf("recording costs were not split: in-window=%s drain=%s", sample.recording, sample.drainRecording)
	}
}

func TestForegroundAPIWindowDrainCompletesRealKVPut(t *testing.T) {
	core := mustCore(t, "n1", []quepaxa.Member{{ID: "n1"}}, nil, nil)
	material, err := materializer.Open(filepath.Join(t.TempDir(), "db.sqlite"), 1)
	if err != nil {
		t.Fatal(err)
	}
	defer material.Close()
	server := NewServer(core, material, "cluster", true, nil)
	defer server.Close()

	const requestID = "foreground-window-drain"
	unlock, err := server.lockRequest(context.Background(), requestID)
	if err != nil {
		t.Fatal(err)
	}
	locked := true
	releaseLock := func() {
		if locked {
			unlock()
			locked = false
		}
	}
	defer releaseLock()
	deadline := time.Now().Add(40 * time.Millisecond)
	gate := &foregroundAPIWindowGate{deadline: deadline}
	started := make(chan struct{})
	type callResult struct {
		response   KVMutationResponse
		start, end time.Time
		err        error
		admitted   bool
	}
	result := make(chan callResult, 1)
	go func() {
		var response KVMutationResponse
		start, end, err, admitted := runForegroundAPICall(context.Background(), gate, foregroundAPICallMaxDuration, func(ctx context.Context) error {
			close(started)
			response, err = server.KVPut(ctx, KVMutationRequest{RequestID: requestID, Key: "window-drain", Value: []byte("durable")})
			return err
		}, nil)
		result <- callResult{response: response, start: start, end: end, err: err, admitted: admitted}
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("KVPut did not enter")
	}
	timer := time.NewTimer(time.Until(deadline))
	<-timer.C
	gate.close()
	if pending, _ := gate.snapshot(); pending != 1 {
		t.Fatalf("pending at cutoff=%d, want 1", pending)
	}
	releaseLock()
	completed := <-result
	if !completed.admitted || completed.err != nil || !completed.end.After(deadline) {
		t.Fatalf("KVPut result=%+v, want successful completion during bounded drain", completed)
	}
	receipt := completed.response.MutationReceipt
	if receipt.Status != types.MutationCommitted || !receipt.Applied || receipt.Slot == 0 {
		t.Fatalf("KVPut receipt=%+v, want committed/applied nonzero slot", receipt)
	}
	if _, outstanding := gate.snapshot(); outstanding != 0 {
		t.Fatalf("KVPut drain left %d calls outstanding", outstanding)
	}
}

func TestForegroundAPISeedDeadlineReservesWindowsAndHonorsParent(t *testing.T) {
	runnerDeadline := time.Unix(2000, 0)
	want := runnerDeadline.Add(-(foregroundAPIWindowTotal + foregroundAPIWindowCleanupReserve))
	ctx, cancel, got, runnerBound := withForegroundAPISeedBudget(context.Background(), runnerDeadline)
	defer cancel()
	actual, ok := ctx.Deadline()
	if !ok || !actual.Equal(want) || !got.Equal(want) || !runnerBound {
		t.Fatalf("derived seed deadline=(%s, ok=%t, runnerBound=%t), want %s", actual, ok, runnerBound, want)
	}
	for _, test := range []struct {
		name           string
		parentDeadline time.Time
		hasParent      bool
		runnerDeadline time.Time
		wantDeadline   time.Time
		wantRunner     bool
	}{
		{name: "runner reserve", runnerDeadline: runnerDeadline, wantDeadline: want, wantRunner: true},
		{name: "earlier parent wins", parentDeadline: want.Add(-time.Second), hasParent: true, runnerDeadline: runnerDeadline, wantDeadline: want.Add(-time.Second)},
		{name: "later parent does not shorten reserve", parentDeadline: want.Add(time.Second), hasParent: true, runnerDeadline: runnerDeadline, wantDeadline: want, wantRunner: true},
		{name: "no runner deadline leaves parent inherited", parentDeadline: runnerDeadline, hasParent: true, wantRunner: false},
	} {
		t.Run(test.name, func(t *testing.T) {
			gotDeadline, gotRunner := foregroundAPISeedDeadline(test.parentDeadline, test.hasParent, test.runnerDeadline)
			if !gotDeadline.Equal(test.wantDeadline) || gotRunner != test.wantRunner {
				t.Fatalf("seed deadline=(%s, runnerBound=%t), want (%s, runnerBound=%t)", gotDeadline, gotRunner, test.wantDeadline, test.wantRunner)
			}
		})
	}
}

func TestForegroundAPISeedBudgetExpiryIsNamedBeforeDispatch(t *testing.T) {
	deadline := time.Now().Add(-time.Second)
	ctx, cancel := context.WithDeadline(context.Background(), deadline)
	defer cancel()
	var calls atomic.Uint64
	err := seedForegroundAPIKeys(ctx, func(context.Context, KVMutationRequest) error {
		calls.Add(1)
		return nil
	}, make([]byte, foregroundAPIValue), 45, deadline, true, nil)
	if !errors.Is(err, errForegroundAPISeedBudgetExpired) || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expired seed budget error=%v, want named budget and deadline causes", err)
	}
	if got := calls.Load(); got != 0 {
		t.Fatalf("expired seed budget dispatched %d KVPut calls", got)
	}
}

// TestForegroundAPICost uses the existing embedded three-peer API harness.
// It is opt-in because each measured window is fixed at two minutes.
func TestForegroundAPICost(t *testing.T) {
	runnerDeadline, hasRunnerDeadline := t.Deadline()
	scenario := os.Getenv("RHIZA_FOREGROUND_API_COST_SCENARIO")
	if scenario == "" {
		t.Skip("set RHIZA_FOREGROUND_API_COST_SCENARIO to run the 150-second local API measurement")
	}
	if scenario != "baseline" && scenario != "checkpoint-active" && scenario != "gc-active" {
		t.Fatalf("unsupported scenario %q", scenario)
	}
	repetition := os.Getenv("RHIZA_FOREGROUND_API_COST_REPETITION")
	if repetition == "" {
		repetition = "1"
	}
	if repetition != "1" && repetition != "2" && repetition != "3" {
		t.Fatalf("repetition must be 1, 2, or 3; got %q", repetition)
	}
	root := t.TempDir()
	archiveBucket, err := objmetrics.NewBucket(objmetrics.Config{Provider: objmetrics.ProviderFilesystem, FilesystemDir: root + "/archive-objects"})
	if err != nil {
		t.Fatal(err)
	}
	defer archiveBucket.Close()
	cluster := newForegroundAPIPeers(t, filepath.Join(root, "peers"))
	defer cluster.close()
	learnObservation := &foregroundLearnObservation{}
	installForegroundLearnHook(t, learnObservation)
	server := cluster.servers["n1"]
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	maintenanceCtx, cancelMaintenance := context.WithCancel(ctx)
	archive := recovery.NewManager(archiveBucket, "foreground-api/archive", 1)
	defer archive.Close()
	if err := archive.Load(ctx); err != nil {
		t.Fatal(err)
	}
	barrierDiagnostic := &foregroundPUTBarrierObservation{}
	putDiagnostic := &foregroundPUTFailureDiagnostic{
		barrier: barrierDiagnostic, materials: cluster.materials["n1"], core: cluster.cores["n1"], archive: archive,
		configuredNodes: foregroundPUTConfiguredNodes(cluster),
	}
	server.SetDurabilityBarrier(func(barrierCtx context.Context, slot quepaxa.Slot) error {
		return barrierDiagnostic.run(barrierCtx, slot, func(ctx context.Context, through quepaxa.Slot) error {
			return archive.SyncThrough(ctx, cluster.cores["n1"], through)
		})
	})
	value := make([]byte, foregroundAPIValue)
	for i := range value {
		value[i] = byte((i*31 + 7) % 251)
	}
	var requestSequence atomic.Uint64
	clientID := time.Now().UnixNano()
	if hasRunnerDeadline {
		requireForegroundAPIWindowBudget(t, runnerDeadline, "before key seeding")
	}
	barrierDiagnostic.setEnabled(true)
	seedCtx, cancelSeed, seedDeadline, runnerBound := withForegroundAPISeedBudget(ctx, runnerDeadline)
	err = seedForegroundAPIKeys(seedCtx, func(putCtx context.Context, request KVMutationRequest) error {
		started := time.Now()
		_, err := server.KVPut(putCtx, request)
		if err != nil {
			putDiagnostic.record("seed", putCtx, request.RequestID, started, time.Now(), time.Time{}, err)
		}
		return err
	}, value, clientID, seedDeadline, runnerBound, func(count int) {
		t.Logf("foreground API seed progress %d/%d", count, foregroundAPIKeys)
	})
	cancelSeed()
	if err != nil {
		putDiagnostic.inspectAfterJoin()
		logForegroundPUTDiagnostic(t, putDiagnostic)
		if errors.Is(err, errForegroundAPISeedBudgetExpired) {
			t.Fatalf("foreground API seed budget expired before completion: %v", err)
		}
		t.Fatalf("seed foreground API keys: %v", err)
	}
	maintenanceActive := atomic.Bool{}
	maintenanceEpoch := atomic.Uint64{}
	var maintenanceErrors atomic.Uint64
	firstMaintenanceError := "none"
	firstLearnDiagnostic := "none"
	var checkpointPublications atomic.Uint64
	checkpointBarrierStats := &foregroundCheckpointBarrierStats{}
	var maintenanceWG sync.WaitGroup
	var checkpointBucket *objmetrics.MeteredBucket
	var checkpointManager *checkpoint.Manager
	var checkpointer *checkpoint.AutoCheckpointer
	stopMaintenance := func() { stopForegroundMaintenance(cancelMaintenance, &maintenanceWG) }
	defer func() {
		stopMaintenance()
		if checkpointBucket != nil {
			_ = checkpointBucket.Close()
		}
	}()
	checkpointMaintenance, gcMaintenance := "not run", "not run"
	if scenario == "checkpoint-active" {
		checkpointBucket, err = objmetrics.NewBucket(objmetrics.Config{Provider: objmetrics.ProviderFilesystem, FilesystemDir: root + "/checkpoint-objects"})
		if err != nil {
			t.Fatal(err)
		}
		checkpointManager, err = configureForegroundCheckpoint(t, cluster, checkpointBucket, filepath.Join(root, "checkpoint-work"))
		if err != nil {
			t.Fatal(err)
		}
		checkpointer = foregroundCheckpointPublisher(cluster, checkpointManager, archive, checkpointBarrierStats)
		checkpointMaintenance = "full publisher claim, snapshot/upload, peer prepare quorum, certified seal, CURRENT CAS promotion, and fresh-manager readback verification"
	}
	if scenario == "gc-active" {
		gcMaintenance = "recovery-manager archive Cleanup; checkpoint-root GC excluded"
	}
	if hasRunnerDeadline {
		requireForegroundAPIWindowBudget(t, runnerDeadline, "before warm-up")
	}
	if scenario != "baseline" {
		maintenanceWG.Add(1)
		go func() {
			defer maintenanceWG.Done()
			ticker := time.NewTicker(5 * time.Second)
			defer ticker.Stop()
			for {
				select {
				case <-maintenanceCtx.Done():
					return
				case <-ticker.C:
					maintenanceEpoch.Add(1)
					maintenanceActive.Store(true)
					maintenanceStarted := time.Now()
					if scenario == "checkpoint-active" {
						created, err := createForegroundCertifiedCheckpoint(maintenanceCtx, checkpointer, checkpointManager, checkpointBucket, archive, cluster)
						if err != nil {
							observation, counted := foregroundMaintenanceFailure(scenario, "checkpoint-maintenance", err, maintenanceCtx, time.Since(maintenanceStarted))
							if counted {
								maintenanceErrors.Add(1)
								if firstMaintenanceError == "none" {
									firstMaintenanceError = observation
									t.Logf("FOREGROUND_MAINTENANCE_ERROR %s", firstMaintenanceError)
									firstLearnDiagnostic = foregroundLearnObservationSummary(cluster, learnObservation)
									if firstLearnDiagnostic != "none" {
										t.Logf("FOREGROUND_LEARN_FAILURE %s", firstLearnDiagnostic)
									}
								}
							}
						} else if created {
							checkpointPublications.Add(1)
						}
					}
					if scenario == "gc-active" {
						if err := archive.Cleanup(maintenanceCtx, 0); err != nil {
							observation, counted := foregroundMaintenanceFailure(scenario, "archive-cleanup", err, maintenanceCtx, time.Since(maintenanceStarted))
							if counted {
								maintenanceErrors.Add(1)
								if firstMaintenanceError == "none" {
									firstMaintenanceError = observation
									t.Logf("FOREGROUND_MAINTENANCE_ERROR %s", firstMaintenanceError)
								}
							}
						}
					}
					maintenanceActive.Store(false)
					maintenanceEpoch.Add(1)
				}
			}
		}()
	}
	if _, _, _, err := runForegroundAPIWindow(ctx, server, value, clientID, &requestSequence, 30*time.Second, &maintenanceActive, &maintenanceEpoch, false, "warmup", putDiagnostic); err != nil {
		putDiagnostic.inspectAfterJoin()
		logForegroundPUTDiagnostic(t, putDiagnostic)
		t.Fatal(err)
	}
	put, get, windowTotals, err := runForegroundAPIWindow(ctx, server, value, clientID, &requestSequence, 120*time.Second, &maintenanceActive, &maintenanceEpoch, true, "measurement", putDiagnostic)
	logForegroundPUTDiagnostic(t, putDiagnostic)
	if err != nil {
		t.Fatal(err)
	}
	stopMaintenance()
	putStats, getStats := put.snapshot(), get.snapshot()
	putJSON := apiLatencyJSON(putStats, 120)
	getJSON := apiLatencyJSON(getStats, 120)
	result, err := json.Marshal(map[string]any{
		"profile": "in-process-api-entry-to-return", "scenario": scenario, "repetition": repetition,
		"topology":               "three embedded API servers with real peer QUIC over loopback UDP; API calls measured directly",
		"durability":             "filesystem object-store before-ack shared archive barrier",
		"checkpoint_maintenance": checkpointMaintenance, "gc_maintenance": gcMaintenance,
		"workers": foregroundAPIWorkers, "seeded_keys": foregroundAPIKeys, "value_bytes": foregroundAPIValue,
		"warmup": "30s", "measured_window": "120s", "application_retries": 0,
		"sample_threshold": 10000, "insufficient_samples": len(putStats.success) < 10000 || len(getStats.success) < 10000,
		"put": putJSON, "linearizable_get": getJSON,
		"window_boundary": map[string]any{
			"starts":                 putStats.started + getStats.started,
			"completed_in_window":    len(putStats.all) + len(getStats.all),
			"completed_in_drain":     len(putStats.drainAll) + len(getStats.drainAll),
			"outstanding_at_cutoff":  windowTotals.pendingAtCutoff,
			"outstanding_after_join": windowTotals.outstanding,
			"maximum_call_duration":  foregroundAPICallMaxDuration.String(),
		},
		"total_successful_ops_per_second":     float64(len(putStats.success)+len(getStats.success)) / 120,
		"total_all_completion_ops_per_second": float64(len(putStats.all)+len(getStats.all)) / 120,
		"maintenance_errors":                  maintenanceErrors.Load(), "maintenance_first_error": firstMaintenanceError,
		"learn_failure_diagnostic":              firstLearnDiagnostic,
		"checkpoint_publications_verified":      checkpointPublications.Load(),
		"checkpoint_barrier_attempts":           checkpointBarrierStats.attempts.Load(),
		"checkpoint_barrier_retries":            checkpointBarrierStats.retries.Load(),
		"checkpoint_barrier_admission_refusals": checkpointBarrierStats.admissionRefusals.Load(),
		"checkpoint_barrier_successes":          checkpointBarrierStats.successes.Load(),
		"checkpoint_barrier_failures":           checkpointBarrierStats.failures.Load(),
		"storage":                               "local filesystem object-store fixture",
		"go":                                    runtime.Version(), "goos": runtime.GOOS, "goarch": runtime.GOARCH, "gomaxprocs": runtime.GOMAXPROCS(0),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("FOREGROUND_API_COST %s", result)
	if scenario == "checkpoint-active" && checkpointPublications.Load() == 0 {
		t.Fatal("checkpoint-active scenario completed without a certified checkpoint publication")
	}
	if maintenanceErrors.Load() != 0 {
		t.Fatalf("maintenance failed %d times; see cost record", maintenanceErrors.Load())
	}
}

func configureForegroundCheckpoint(t testing.TB, peers *foregroundAPIPeers, bucket *objmetrics.MeteredBucket, workRoot string) (*checkpoint.Manager, error) {
	t.Helper()
	for id, core := range peers.cores {
		localDir := filepath.Join(workRoot, string(id))
		if err := os.MkdirAll(localDir, 0o700); err != nil {
			return nil, err
		}
		manager := checkpoint.NewManager(bucket, "foreground-api/checkpoint", localDir, 1)
		if err := manager.Load(context.Background()); err != nil {
			return nil, err
		}
		core.SetCheckpointValidator(func(ctx context.Context, seal quepaxa.CheckpointSeal) error {
			return manager.Verify(ctx, uint64(seal.Index), seal.RootHash, seal.StateHash)
		})
		peers.checkpoints[id] = manager
	}
	return peers.checkpoints["n1"], nil
}

func retryForegroundCheckpointBarrier(ctx context.Context, value []byte, stats *foregroundCheckpointBarrierStats, propose func(context.Context, []byte) error) error {
	for attempt := 0; attempt < foregroundCheckpointBarrierMaxAttempts; attempt++ {
		if attempt > 0 {
			stats.retries.Add(1)
		}
		stats.attempts.Add(1)
		err := propose(ctx, value)
		if err == nil {
			stats.successes.Add(1)
			return nil
		}
		stats.failures.Add(1)
		var admissionErr proposalAdmissionOverload
		if !errors.As(err, &admissionErr) {
			return err
		}
		stats.admissionRefusals.Add(1)
		if attempt+1 == foregroundCheckpointBarrierMaxAttempts {
			return err
		}
		timer := time.NewTimer(foregroundCheckpointBarrierRetryDelay)
		select {
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		}
	}
	return proposalAdmissionOverload{}
}

func TestForegroundCheckpointBarrierRetriesOnlyTypedPreAdmissionOverload(t *testing.T) {
	t.Run("same barrier payload retries after admission refusal", func(t *testing.T) {
		ctx := context.Background()
		stats := &foregroundCheckpointBarrierStats{}
		value := []byte("same-read-barrier")
		var calls int
		err := retryForegroundCheckpointBarrier(ctx, value, stats, func(_ context.Context, got []byte) error {
			calls++
			if !bytes.Equal(got, value) {
				t.Fatalf("retry payload=%q, want unchanged %q", got, value)
			}
			if calls == 1 {
				return fmt.Errorf("wrapped advance: %w", proposalAdmissionOverload{})
			}
			return nil
		})
		if err != nil || calls != 2 || stats.attempts.Load() != 2 || stats.retries.Load() != 1 || stats.admissionRefusals.Load() != 1 {
			t.Fatalf("err=%v calls=%d attempts=%d retries=%d refusals=%d", err, calls, stats.attempts.Load(), stats.retries.Load(), stats.admissionRefusals.Load())
		}
	})

	for _, test := range []struct {
		name string
		err  error
	}{
		{name: "untyped overload", err: ErrOverloaded},
		{name: "commit outcome unknown", err: fmt.Errorf("%w: proposal already admitted", ErrCommitUnknown)},
		{name: "fencing", err: errors.New("publisher fenced")},
	} {
		t.Run(test.name+" is not retried", func(t *testing.T) {
			stats := &foregroundCheckpointBarrierStats{}
			calls := 0
			err := retryForegroundCheckpointBarrier(context.Background(), []byte("barrier"), stats, func(context.Context, []byte) error {
				calls++
				return test.err
			})
			if !errors.Is(err, test.err) || calls != 1 || stats.attempts.Load() != 1 || stats.retries.Load() != 0 {
				t.Fatalf("err=%v calls=%d attempts=%d retries=%d", err, calls, stats.attempts.Load(), stats.retries.Load())
			}
		})
	}

	t.Run("cancellation during retry wait", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		stats := &foregroundCheckpointBarrierStats{}
		calls := 0
		err := retryForegroundCheckpointBarrier(ctx, []byte("barrier"), stats, func(context.Context, []byte) error {
			calls++
			cancel()
			return proposalAdmissionOverload{}
		})
		if !errors.Is(err, context.Canceled) || calls != 1 || stats.attempts.Load() != 1 || stats.retries.Load() != 0 {
			t.Fatalf("err=%v calls=%d attempts=%d retries=%d", err, calls, stats.attempts.Load(), stats.retries.Load())
		}
	})

	t.Run("attempt cap preserves overload cause", func(t *testing.T) {
		stats := &foregroundCheckpointBarrierStats{}
		calls := 0
		err := retryForegroundCheckpointBarrier(context.Background(), []byte("barrier"), stats, func(context.Context, []byte) error {
			calls++
			return proposalAdmissionOverload{}
		})
		var admissionErr proposalAdmissionOverload
		if !errors.As(err, &admissionErr) || calls != foregroundCheckpointBarrierMaxAttempts || stats.attempts.Load() != foregroundCheckpointBarrierMaxAttempts || stats.retries.Load() != foregroundCheckpointBarrierMaxAttempts-1 {
			t.Fatalf("err=%v calls=%d attempts=%d retries=%d", err, calls, stats.attempts.Load(), stats.retries.Load())
		}
	})
}

func foregroundCheckpointPublisher(peers *foregroundAPIPeers, manager *checkpoint.Manager, archive *recovery.Manager, barrierStats *foregroundCheckpointBarrierStats) *checkpoint.AutoCheckpointer {
	core, server, material := peers.cores["n1"], peers.servers["n1"], peers.materials["n1"]
	transport := peers.transports[0]
	auto := checkpoint.NewAutoCheckpointer(manager, material, 1, time.Hour)
	auto.ConfigurePublisher(string(core.NodeID()), func() uint64 {
		floor := material.Tip()
		if seal, ok, err := core.LatestCheckpointSeal(); err == nil && ok {
			floor = max(floor, uint64(seal.Index))
		}
		if latest := manager.Latest(); latest != nil {
			floor = max(floor, latest.Index)
		}
		return floor
	}, func(ctx context.Context, reserved uint64) error {
		for material.Tip() < reserved {
			var nonce [types.ReadBarrierNonceSize]byte
			if _, err := rand.Read(nonce[:]); err != nil {
				return err
			}
			if err := retryForegroundCheckpointBarrier(ctx, types.EncodeReadBarrier(nonce), barrierStats, func(proposalCtx context.Context, value []byte) error {
				_, err := server.ProposeControl(proposalCtx, value)
				if err != nil {
					return fmt.Errorf("read-barrier-advance: %w", err)
				}
				return err
			}); err != nil {
				return err
			}
		}
		return nil
	})
	auto.ConfigurePublication(func() bool {
		seal, ok, err := core.LatestCheckpointSeal()
		return core.IsVoter() && err == nil && (!ok || material.Tip() > uint64(seal.Index))
	}, func(ctx context.Context, root *checkpoint.Checkpoint) error {
		prefix, ok := core.PrefixHash(quepaxa.Slot(root.Index))
		if !ok {
			return fmt.Errorf("checkpoint prefix %d is unavailable", root.Index)
		}
		next, following, err := core.CheckpointLeaderOrders(quepaxa.Slot(root.Index))
		if err != nil {
			return err
		}
		seal := quepaxa.CheckpointSeal{
			ConfigID: core.ConfigIDForSlot(quepaxa.Slot(root.Index)), Index: quepaxa.Slot(root.Index),
			RootHash: root.RootHash, StateHash: root.Hash, PrefixHash: prefix,
			NextLeaderOrder: next, FollowingLeaderOrder: following, GenerationAnchorHash: core.GenerationAnchorHash(),
		}
		if err := manager.ValidatePublisherClaim(ctx, string(core.NodeID()), root.Index, root.RootHash); err != nil {
			return err
		}
		if err := core.PrepareCheckpoint(ctx, seal); err != nil {
			return err
		}
		if err := transport.PrepareCheckpoint(ctx, seal); err != nil {
			return err
		}
		value, err := quepaxa.EncodeCheckpointSeal(seal)
		if err != nil {
			return err
		}
		sealSlot, _, err := core.Propose(ctx, value)
		if err != nil {
			return fmt.Errorf("checkpoint-seal-proposal: %w", err)
		}
		if err := server.applyDecisions(ctx, sealSlot); err != nil {
			return err
		}
		if err := archive.SyncThrough(ctx, core, core.Tip()); err != nil {
			return err
		}
		if err := manager.PromoteCertifiedCurrent(ctx, root); err != nil {
			return err
		}
		sealed, ok, err := core.LatestCheckpointSeal()
		if err != nil {
			return err
		}
		if !ok || sealed.Index <= core.CompactionFloor() {
			return nil
		}
		if quepaxa.Slot(material.Tip()) < sealed.Index {
			return fmt.Errorf("materializer tip %d is behind checkpoint %d", material.Tip(), sealed.Index)
		}
		if err := core.VerifyCheckpoint(ctx, sealed.CheckpointSeal); err != nil {
			return err
		}
		if err := archive.SyncThrough(ctx, core, sealed.DecisionSlot); err != nil {
			return err
		}
		decision, ok := core.CertifiedValue(sealed.DecisionSlot)
		if !ok {
			return fmt.Errorf("checkpoint seal decision %d is unavailable", sealed.DecisionSlot)
		}
		if err := archive.TrimThrough(ctx, sealed, decision); err != nil {
			return err
		}
		return core.CompactThrough(sealed.Index, sealed.RootHash)
	})
	return auto
}

func createForegroundCertifiedCheckpoint(ctx context.Context, auto *checkpoint.AutoCheckpointer, manager *checkpoint.Manager, bucket *objmetrics.MeteredBucket, archive *recovery.Manager, peers *foregroundAPIPeers) (bool, error) {
	core, material := peers.cores["n1"], peers.materials["n1"]
	before := manager.Latest()
	if material.Tip() == 0 || before != nil && before.Index >= material.Tip() {
		return false, nil
	}
	if err := archive.SyncThrough(ctx, core, core.Tip()); err != nil {
		return false, fmt.Errorf("checkpoint-pre-sync: %w", err)
	}
	if err := auto.CheckpointOnShutdown(ctx, material.Tip()); err != nil {
		return false, fmt.Errorf("checkpoint-publication: %w", err)
	}
	winner := manager.Latest()
	if winner == nil || before != nil && (winner.Index <= before.Index || winner.RootHash == before.RootHash) {
		return false, nil
	}
	seal, sealed, err := core.LatestCheckpointSeal()
	if err != nil {
		return false, fmt.Errorf("certified-seal-read: %w", err)
	}
	if !sealed || uint64(seal.Index) != winner.Index || seal.RootHash != winner.RootHash || seal.StateHash != winner.Hash {
		return false, fmt.Errorf("certified-seal-validation: checkpoint CURRENT candidate %d is not certified by the local Core", winner.Index)
	}
	readback := checkpoint.NewManager(bucket, "foreground-api/checkpoint", "", 1)
	if err := readback.Load(ctx); err != nil {
		return false, fmt.Errorf("fresh-current-readback: %w", err)
	}
	verified := readback.Latest()
	if verified == nil || verified.Index != winner.Index || verified.RootHash != winner.RootHash || verified.Hash != winner.Hash {
		return false, fmt.Errorf("fresh-current-comparison: checkpoint CURRENT readback does not match certified winner %d", winner.Index)
	}
	return true, nil
}

func TestForegroundAPICheckpointUsesCertifiedLoopbackPublication(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	root := t.TempDir()
	archiveBucket, err := objmetrics.NewBucket(objmetrics.Config{Provider: objmetrics.ProviderFilesystem, FilesystemDir: filepath.Join(root, "archive-objects")})
	if err != nil {
		t.Fatal(err)
	}
	defer archiveBucket.Close()
	peers := newForegroundAPIPeers(t, filepath.Join(root, "peers"))
	defer peers.close()
	archive := recovery.NewManager(archiveBucket, "foreground-api/archive", 1)
	defer archive.Close()
	if err := archive.Load(ctx); err != nil {
		t.Fatal(err)
	}
	server, core := peers.servers["n1"], peers.cores["n1"]
	server.SetDurabilityBarrier(func(barrierCtx context.Context, slot quepaxa.Slot) error {
		return archive.SyncThrough(barrierCtx, core, slot)
	})
	checkpointBucket, err := objmetrics.NewBucket(objmetrics.Config{Provider: objmetrics.ProviderFilesystem, FilesystemDir: filepath.Join(root, "checkpoint-objects")})
	if err != nil {
		t.Fatal(err)
	}
	defer checkpointBucket.Close()
	manager, err := configureForegroundCheckpoint(t, peers, checkpointBucket, filepath.Join(root, "checkpoint-work"))
	if err != nil {
		t.Fatal(err)
	}
	barrierStats := &foregroundCheckpointBarrierStats{}
	checkpointer := foregroundCheckpointPublisher(peers, manager, archive, barrierStats)
	for i, requestID := range []string{"foreground-checkpoint-fixture-1", "foreground-checkpoint-fixture-2"} {
		if _, err := server.KVPut(ctx, KVMutationRequest{RequestID: requestID, Key: "key", Value: []byte(fmt.Sprintf("value-%d", i))}); err != nil {
			t.Fatal(err)
		}
		if i == 0 {
			if len(server.localCap) != 0 {
				t.Fatalf("local proposal permits unexpectedly occupied before fixture setup: %d", len(server.localCap))
			}
			for range cap(server.localCap) {
				server.localCap <- struct{}{}
			}
			publication := make(chan struct {
				created bool
				err     error
			}, 1)
			go func() {
				created, err := createForegroundCertifiedCheckpoint(ctx, checkpointer, manager, checkpointBucket, archive, peers)
				publication <- struct {
					created bool
					err     error
				}{created: created, err: err}
			}()
			waitUntil := time.Now().Add(2 * time.Second)
			for barrierStats.admissionRefusals.Load() == 0 && time.Now().Before(waitUntil) {
				time.Sleep(time.Millisecond)
			}
			sawRefusal := barrierStats.admissionRefusals.Load() > 0
			for range cap(server.localCap) {
				<-server.localCap
			}
			result := <-publication
			if result.err != nil {
				t.Fatal(result.err)
			}
			if !result.created {
				t.Fatalf("checkpoint publication helper did not publish certified CURRENT %d", i+1)
			}
			if !sawRefusal {
				t.Fatal("checkpoint advance did not report the held proposal admission slots")
			}
			if barrierStats.attempts.Load() < 2 || barrierStats.retries.Load() == 0 {
				t.Fatalf("barrier attempts=%d retries=%d; want an observed admission retry", barrierStats.attempts.Load(), barrierStats.retries.Load())
			}
			continue
		}
		created, err := createForegroundCertifiedCheckpoint(ctx, checkpointer, manager, checkpointBucket, archive, peers)
		if err != nil {
			t.Fatal(err)
		}
		if !created {
			t.Fatalf("checkpoint publication helper did not publish certified CURRENT %d", i+1)
		}
	}
}

type foregroundAPIWindowTotals struct {
	pendingAtCutoff int
	outstanding     int
}

func runForegroundAPIWindow(ctx context.Context, server *Server, value []byte, clientID int64, sequence *atomic.Uint64, duration time.Duration, maintenanceActive *atomic.Bool, maintenanceEpoch *atomic.Uint64, collect bool, phase string, diagnostic *foregroundPUTFailureDiagnostic) (*apiLatencyAccumulator, *apiLatencyAccumulator, foregroundAPIWindowTotals, error) {
	put, get := &apiLatencyAccumulator{}, &apiLatencyAccumulator{}
	if diagnostic != nil && diagnostic.barrier != nil {
		diagnostic.barrier.setEnabled(true)
	}
	window, cancel := context.WithTimeout(ctx, duration)
	defer cancel()
	deadline, _ := window.Deadline()
	gate := &foregroundAPIWindowGate{deadline: deadline}
	var workers sync.WaitGroup
	for worker := range foregroundAPIWorkers {
		workers.Add(1)
		go func(worker int) {
			defer workers.Done()
			index := worker
			for {
				key := fmt.Sprintf("key-%04d", index%foregroundAPIKeys)
				id := sequence.Add(1)
				startedEpoch := maintenanceEpoch.Load()
				startedDuringMaintenance := maintenanceActive.Load()
				requestID := fmt.Sprintf("foreground-%d-%d", clientID, id)
				started, completed, putErr, admitted := runForegroundAPICall(ctx, gate, foregroundAPICallMaxDuration, func(callCtx context.Context) error {
					_, err := server.KVPut(callCtx, KVMutationRequest{RequestID: requestID, Key: key, Value: value})
					return err
				}, func(callCtx context.Context, callStarted, callCompleted time.Time, err error) {
					if diagnostic != nil {
						diagnostic.record(phase, callCtx, requestID, callStarted, callCompleted, deadline, err)
					}
				})
				if !admitted {
					return
				}
				putElapsed := completed.Sub(started)
				putOverlap := startedDuringMaintenance || maintenanceActive.Load() || startedEpoch != maintenanceEpoch.Load()
				if collect {
					put.add(putElapsed, putErr, putOverlap, !completed.After(deadline))
				}
				startedEpoch = maintenanceEpoch.Load()
				startedDuringMaintenance = maintenanceActive.Load()
				started, completed, getErr, admitted := runForegroundAPICall(ctx, gate, foregroundAPICallMaxDuration, func(callCtx context.Context) error {
					response, err := server.KVGet(callCtx, KVGetRequest{Key: key, Consistency: "linearizable"})
					if err == nil && !response.Found {
						return fmt.Errorf("linearizable get missing seeded value")
					}
					return err
				}, nil)
				if !admitted {
					return
				}
				getElapsed := completed.Sub(started)
				getOverlap := startedDuringMaintenance || maintenanceActive.Load() || startedEpoch != maintenanceEpoch.Load()
				if collect {
					get.add(getElapsed, getErr, getOverlap, !completed.After(deadline))
				}
				index = (index + foregroundAPIWorkers) % foregroundAPIKeys
			}
		}(worker)
	}
	<-window.Done()
	gate.close()
	workers.Wait()
	if diagnostic != nil && phase == "measurement" {
		diagnostic.inspectAfterJoin()
	}
	if err := ctx.Err(); err != nil {
		pending, outstanding := gate.snapshot()
		return put, get, foregroundAPIWindowTotals{pendingAtCutoff: pending, outstanding: outstanding}, err
	}
	pending, outstanding := gate.snapshot()
	if collect {
		putSample, getSample := put.snapshot(), get.snapshot()
		completed := len(putSample.all) + len(putSample.drainAll) + len(getSample.all) + len(getSample.drainAll)
		started := putSample.started + getSample.started
		if completed != started || outstanding != 0 {
			return put, get, foregroundAPIWindowTotals{pendingAtCutoff: pending, outstanding: outstanding}, fmt.Errorf("foreground API window accounting mismatch: started=%d completed=%d outstanding=%d", started, completed, outstanding)
		}
	}
	return put, get, foregroundAPIWindowTotals{pendingAtCutoff: pending, outstanding: outstanding}, nil
}

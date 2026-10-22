package network

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/materializer"
	"github.com/mrchypark/rhiza/pkg/qlog"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/mrchypark/rhiza/pkg/recovery"
)

type readGateWriter struct {
	header http.Header
	mu     sync.Mutex
	body   bytes.Buffer
	wrote  chan struct{}
	once   sync.Once
	status int
}

func TestManagedReadBarrierActivatesAfterLiveLearnerPromotion(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	initial := quepaxa.Cluster{ConfigID: 1, Members: []quepaxa.Member{testMember("cluster", "a", "a-token")}}
	learnerWAL, err := qlog.Open(t.TempDir() + "/learner-qlog")
	if err != nil {
		t.Fatal(err)
	}
	defer learnerWAL.Close()
	learner, err := quepaxa.NewLearner(quepaxa.Config{NodeID: "b", Cluster: initial, WAL: learnerWAL, EnableReconfiguration: true})
	if err != nil {
		t.Fatal(err)
	}

	sourceWAL, err := qlog.Open(t.TempDir() + "/source-qlog")
	if err != nil {
		t.Fatal(err)
	}
	defer sourceWAL.Close()
	source, err := quepaxa.New(quepaxa.Config{
		NodeID: "a", Cluster: initial, WAL: sourceWAL, EnableReconfiguration: true,
		ReconfigurationAdmission: func(context.Context, quepaxa.Cluster, quepaxa.Slot, [32]byte) error { return nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	joiner := testMember("cluster", "b", "b-token")
	joiner.WALIdentity = learner.WALIdentity()
	target := quepaxa.Cluster{ConfigID: 2, Members: append(append([]quepaxa.Member(nil), initial.Members...), joiner)}
	if _, err := source.BeginReconfiguration(ctx, target); err != nil {
		t.Fatal(err)
	}
	if err := source.FinishReconfiguration(ctx); err != nil {
		t.Fatal(err)
	}
	values, _, err := source.DecisionsFrom(1, 128)
	if err != nil {
		t.Fatal(err)
	}
	peer := startCatchUpPeer(t, "a", values, nil)
	peerConfig := &quepaxa.Cluster{ConfigID: 1, Members: []quepaxa.Member{peer.member}}
	transport := adminTransport("cluster", "b", peerConfig, "admin")
	defer transport.Close()

	material, err := materializer.Open(t.TempDir()+"/promoted.sqlite", 1)
	if err != nil {
		t.Fatal(err)
	}
	defer material.Close()
	server := NewServer(learner, material, "cluster", false, transport)
	defer server.Close()
	var barrierCalls int
	server.SetReadDurabilityBarrier(func(context.Context, quepaxa.Slot) error {
		if learner.IsVoter() {
			barrierCalls++
		}
		return nil
	})
	if _, err := server.Query(ctx, QueryRequest{SQL: "SELECT 1", Consistency: "local"}); err != nil {
		t.Fatal(err)
	}
	if barrierCalls != 0 {
		t.Fatal("learner read unexpectedly entered voter durability barrier")
	}
	if err := server.catchUpFrom(ctx, "a", source.Tip(), true); err != nil {
		t.Fatalf("promote live learner: %v", err)
	}
	if !learner.IsVoter() {
		t.Fatalf("learner did not adopt certified voter configuration: %+v", learner.CurrentCluster())
	}
	if _, err := server.Query(ctx, QueryRequest{SQL: "SELECT 1", Consistency: "local"}); err != nil {
		t.Fatal(err)
	}
	if barrierCalls != 1 {
		t.Fatalf("promoted voter read barrier calls=%d, want 1", barrierCalls)
	}
}

func newReadGateWriter() *readGateWriter {
	return &readGateWriter{header: make(http.Header), wrote: make(chan struct{})}
}

func (w *readGateWriter) Header() http.Header { return w.header }
func (w *readGateWriter) WriteHeader(status int) {
	w.mu.Lock()
	w.status = status
	w.mu.Unlock()
	w.once.Do(func() { close(w.wrote) })
}
func (w *readGateWriter) Write(body []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.status == 0 {
		w.status = http.StatusOK
	}
	w.once.Do(func() { close(w.wrote) })
	return w.body.Write(body)
}

func readGateServer(t *testing.T) *Server {
	t.Helper()
	core := mustCore(t, "n1", []quepaxa.Member{{ID: "n1"}}, nil, nil)
	material, err := materializer.Open(t.TempDir()+"/db.sqlite", 1)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = material.Close() })
	server := NewServer(core, material, "cluster", true, nil)
	t.Cleanup(func() { server.Close() })
	return server
}

func TestManagedReadBarrierCoversSQLKVAndGraphReadAPIs(t *testing.T) {
	server := readGateServer(t)
	var got []quepaxa.Slot
	server.SetReadDurabilityBarrier(func(_ context.Context, slot quepaxa.Slot) error {
		got = append(got, slot)
		return nil
	})

	if _, err := server.Execute(context.Background(), ExecuteRequest{RequestID: "read-gate-create", SQL: "CREATE TABLE read_gate (id INTEGER)"}); err != nil {
		t.Fatal(err)
	}
	if _, err := server.KVGet(context.Background(), KVGetRequest{Key: "missing", Consistency: "local"}); err != nil {
		t.Fatal(err)
	}
	if err := server.material.ConfigureLocalGraphNodePropertyIndexes([]types.GraphNodePropertyIndex{{Label: "N", Property: "id"}}); err != nil {
		t.Fatal(err)
	}
	if _, err := server.GraphQuery(context.Background(), GraphQueryRequest{Cypher: "MATCH (n) RETURN n", Consistency: "local"}); err != nil {
		t.Fatal(err)
	}
	if _, err := server.GraphReachable(context.Background(), GraphReachableRequest{
		StartLabel: "N", StartProperty: "id", StartValue: "missing", EdgeType: "R",
		ResultProperty: "id", MaxDepth: 1, MaxResults: 10, MaxScannedEdges: 10, MaxBytes: 4096,
		Consistency: "local",
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := server.GraphStreamRead(context.Background(), GraphStreamReadRequest{Stream: "changes", Limit: 1, Consistency: "local"}); err != nil {
		t.Fatal(err)
	}
	if _, err := server.GraphStreamOffset(context.Background(), GraphStreamOffsetRequest{Stream: "changes", Consumer: "reader", Consistency: "local"}); err != nil {
		t.Fatal(err)
	}
	if _, err := server.Query(context.Background(), QueryRequest{SQL: "SELECT 1", Consistency: "local"}); err != nil {
		t.Fatal(err)
	}
	if len(got) != 6 {
		t.Fatalf("read barrier calls=%d, want one for SQL, KV, graph query/reachability/stream/offset", len(got))
	}
	for _, slot := range got {
		if slot == 0 {
			t.Fatalf("read barrier received zero applied slot: %v", got)
		}
	}
}

func TestReadDurabilityFailureRejectsSealedEpoch(t *testing.T) {
	server := readGateServer(t)
	server.SetReadDurabilityBarrier(func(context.Context, quepaxa.Slot) error {
		return recovery.ErrArchiveSealed
	})
	response, err := server.Query(context.Background(), QueryRequest{SQL: "SELECT 1", Consistency: "local"})
	if !errors.Is(err, ErrDurabilityUnavailable) || !errors.Is(err, recovery.ErrArchiveSealed) {
		t.Fatalf("response=%+v error=%v; want durability failure wrapping sealed epoch", response, err)
	}
}

func TestHTTPReadWaitsForDurabilityAtObservedSnapshotBeforeWritingResponse(t *testing.T) {
	server := readGateServer(t)
	create, err := server.Execute(context.Background(), ExecuteRequest{RequestID: "read-gate-table", SQL: "CREATE TABLE read_gate_http (id INTEGER)"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := server.Execute(context.Background(), ExecuteRequest{RequestID: "read-gate-first", SQL: "INSERT INTO read_gate_http VALUES (1)"}); err != nil {
		t.Fatal(err)
	}
	entered, release := make(chan quepaxa.Slot, 1), make(chan struct{})
	var releaseOnce sync.Once
	releaseBarrier := func() { releaseOnce.Do(func() { close(release) }) }
	defer releaseBarrier()
	server.SetReadDurabilityBarrier(func(_ context.Context, slot quepaxa.Slot) error {
		entered <- slot
		<-release
		return nil
	})

	writer := newReadGateWriter()
	request := httptest.NewRequest(http.MethodPost, "/sql/query", bytes.NewBufferString(`{"sql":"SELECT id FROM read_gate_http ORDER BY id","consistency":"linearizable"}`))
	done := make(chan struct{})
	go func() {
		server.ServeHTTP(writer, request)
		close(done)
	}()

	var snapshotSlot quepaxa.Slot
	select {
	case snapshotSlot = <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("read did not reach its durability barrier")
	}
	if snapshotSlot <= quepaxa.Slot(create.Slot) {
		t.Fatalf("snapshot slot=%d, expected it to include the concurrent fixture writes", snapshotSlot)
	}
	select {
	case <-writer.wrote:
		t.Fatal("HTTP response began before object-store durability completed")
	default:
	}

	insert, err := server.Execute(context.Background(), ExecuteRequest{RequestID: "read-gate-concurrent", SQL: "INSERT INTO read_gate_http VALUES (2)"})
	if err != nil {
		t.Fatal(err)
	}
	releaseBarrier()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("HTTP read did not complete after releasing durability barrier")
	}
	if snapshotSlot >= quepaxa.Slot(insert.Slot) {
		t.Fatalf("read snapshot slot=%d includes later concurrent apply slot=%d", snapshotSlot, insert.Slot)
	}
	if writer.status != http.StatusOK || !bytes.Contains(writer.body.Bytes(), []byte(`"applied_slot":`)) || bytes.Contains(writer.body.Bytes(), []byte(`"id":2`)) {
		t.Fatalf("HTTP status=%d body=%s; want successful pre-concurrent snapshot", writer.status, writer.body.String())
	}
}

func TestGraphStreamHTTPDoesNotEmitBeforeReadDurability(t *testing.T) {
	server := readGateServer(t)
	entered, release := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	releaseBarrier := func() { releaseOnce.Do(func() { close(release) }) }
	defer releaseBarrier()
	server.SetReadDurabilityBarrier(func(context.Context, quepaxa.Slot) error {
		close(entered)
		<-release
		return nil
	})
	writer := newReadGateWriter()
	request := httptest.NewRequest(http.MethodPost, "/graph/streams/read", bytes.NewBufferString(`{"stream":"changes","limit":1,"consistency":"local"}`))
	done := make(chan struct{})
	go func() {
		server.ServeHTTP(writer, request)
		close(done)
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("stream read did not reach durability barrier")
	}
	select {
	case <-writer.wrote:
		t.Fatal("stream response began before object-store durability completed")
	default:
	}
	releaseBarrier()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("stream read did not complete")
	}
}

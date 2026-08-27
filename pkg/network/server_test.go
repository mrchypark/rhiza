//go:build !graph

package network

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/materializer"
	"github.com/mrchypark/rhiza/pkg/qlog"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

type unavailableTransport struct{}

func (unavailableTransport) SendRecord(context.Context, quepaxa.NodeID, quepaxa.RecordRequest) (quepaxa.Summary, error) {
	return quepaxa.Summary{}, errors.New("unavailable")
}

func (unavailableTransport) SendDecision(context.Context, quepaxa.Decision) error {
	return errors.New("unavailable")
}

func (unavailableTransport) ReadTip(context.Context, quepaxa.NodeID) (quepaxa.Slot, error) {
	return 0, errors.New("unavailable")
}
func (unavailableTransport) StageValue(context.Context, quepaxa.NodeID, quepaxa.ValueHash, []byte) error {
	return errors.New("unavailable")
}
func (unavailableTransport) FetchValue(context.Context, quepaxa.NodeID, quepaxa.ValueHash) ([]byte, error) {
	return nil, errors.New("unavailable")
}

type retryLearnerTransport struct {
	sendCalls int
	sendErr   error
}

func (t *retryLearnerTransport) SendRecord(_ context.Context, to quepaxa.NodeID, request quepaxa.RecordRequest) (quepaxa.Summary, error) {
	proposal := request.Proposal
	return quepaxa.Summary{RecorderID: to, Step: request.Step, FirstCurrent: &proposal}, nil
}
func (t *retryLearnerTransport) SendDecision(context.Context, quepaxa.Decision) error {
	t.sendCalls++
	return t.sendErr
}
func (*retryLearnerTransport) ReadTip(context.Context, quepaxa.NodeID) (quepaxa.Slot, error) {
	return 0, nil
}
func (*retryLearnerTransport) StageValue(context.Context, quepaxa.NodeID, quepaxa.ValueHash, []byte) error {
	return nil
}
func (*retryLearnerTransport) FetchValue(context.Context, quepaxa.NodeID, quepaxa.ValueHash) ([]byte, error) {
	return nil, errors.New("value unavailable")
}

func mustCore(t *testing.T, nodeID quepaxa.NodeID, members []quepaxa.Member, wal *qlog.WAL, transport quepaxa.Transport) *quepaxa.Core {
	t.Helper()
	if wal == nil {
		var err error
		wal, err = qlog.Open(t.TempDir() + "/qlog")
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = wal.Close() })
	}
	if len(members) > 1 && transport == nil {
		transport = unavailableTransport{}
	}
	core, err := quepaxa.New(quepaxa.Config{
		NodeID: nodeID, Cluster: quepaxa.Cluster{Members: members}, WAL: wal, Transport: transport,
	})
	if err != nil {
		t.Fatal(err)
	}
	return core
}

func TestLocalProposerAppliesAlreadyDecidedValue(t *testing.T) {
	wal, err := qlog.Open(t.TempDir() + "/qlog")
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()
	members := []quepaxa.Member{{ID: "n1"}}
	core := mustCore(t, "n1", members, wal, nil)
	material, err := materializer.Open(t.TempDir()+"/db.sqlite", 1)
	if err != nil {
		t.Fatal(err)
	}
	defer material.Close()
	value, err := types.EncodeSQLBatch([]types.SQLCommand{{RequestID: "schema", SQL: "CREATE TABLE applied (id INTEGER)"}})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := core.Propose(context.Background(), value); err != nil {
		t.Fatal(err)
	}
	server := NewServer(core, material, "cluster", true, nil, members, 0)
	if _, err := server.proposeLocal(context.Background(), value); err != nil {
		t.Fatal(err)
	}
	if material.Tip() != 1 {
		t.Fatalf("material tip=%d, want 1", material.Tip())
	}
}

func TestLocalRetryReestablishesLearnerQuorum(t *testing.T) {
	members := []quepaxa.Member{{ID: "n1"}, {ID: "n2"}, {ID: "n3"}}
	transport := &retryLearnerTransport{sendErr: errors.New("learners unavailable")}
	core := mustCore(t, "n1", members, nil, transport)
	material, err := materializer.Open(t.TempDir()+"/db.sqlite", 1)
	if err != nil {
		t.Fatal(err)
	}
	defer material.Close()
	value, err := types.EncodeSQLBatch([]types.SQLCommand{{RequestID: "schema-retry", SQL: "CREATE TABLE retried (id INTEGER)"}})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := core.Propose(context.Background(), value); !errors.Is(err, quepaxa.ErrQuorumUnavailable) {
		t.Fatalf("first proposal error=%v, want quorum unavailable", err)
	}
	transport.sendErr = nil
	server := NewServer(core, material, "cluster", true, nil, members, 0)
	defer server.Close()
	if _, err := server.proposeLocal(context.Background(), value); err != nil {
		t.Fatal(err)
	}
	if transport.sendCalls != 2 || material.Tip() != 1 {
		t.Fatalf("SendDecision calls=%d material tip=%d, want 2 and 1", transport.sendCalls, material.Tip())
	}
}

func TestHTTPAdapterDoesNotExposePeerRPC(t *testing.T) {
	members := []quepaxa.Member{{ID: "n1"}}
	server := NewServer(mustCore(t, "n1", members, nil, nil), nil, "cluster", true, nil, members, 0)
	response := httptest.NewRecorder()
	server.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/internal/decisions?from=1", nil))
	if response.Code != http.StatusNotFound {
		t.Fatalf("status=%d, want 404", response.Code)
	}
}

func TestSQLBuildRejectsNewGraphValueBeforeConsensus(t *testing.T) {
	members := []quepaxa.Member{{ID: "n1"}}
	core := mustCore(t, "n1", members, nil, nil)
	server := NewServer(core, nil, "cluster", true, nil, members, 0)
	defer server.Close()
	value, err := types.EncodeGraphCommand(types.GraphCommand{RequestID: "graph", Cypher: "CREATE (:N)"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := server.proposeLocal(context.Background(), value); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("error=%v, want invalid request", err)
	}
	if core.Tip() != 0 {
		t.Fatalf("rejected graph value advanced consensus tip to %d", core.Tip())
	}
}

func TestDurabilityFailureIsRetryableWithSameRequestID(t *testing.T) {
	members := []quepaxa.Member{{ID: "n1"}}
	core := mustCore(t, "n1", members, nil, nil)
	material, err := materializer.Open(t.TempDir()+"/db.sqlite", 1)
	if err != nil {
		t.Fatal(err)
	}
	defer material.Close()
	server := NewServer(core, material, "cluster", true, nil, members, 0)
	defer server.Close()
	server.SetDurabilityBarrier(func(context.Context, quepaxa.Slot) error {
		return errors.New("bucket unavailable")
	})
	req := ExecuteRequest{RequestID: "schema", SQL: "CREATE TABLE durable (id INTEGER)"}
	if _, err := server.Execute(context.Background(), req); !errors.Is(err, ErrCommitUnknown) {
		t.Fatalf("error=%v, want ErrCommitUnknown", err)
	} else {
		var unknown *CommitUnknownError
		if !errors.As(err, &unknown) || unknown.Slot == 0 || unknown.RequestID != req.RequestID {
			t.Fatalf("commit unknown detail=%#v", unknown)
		}
		status, statusErr := server.RequestStatus(context.Background(), RequestStatusRequest{RequestID: req.RequestID, Slot: uint64(unknown.Slot)})
		if statusErr != nil || status.State != "committed" {
			t.Fatalf("status=%+v err=%v", status, statusErr)
		}
	}

	server.SetDurabilityBarrier(nil)
	response, err := server.Execute(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if response.Slot == 0 {
		t.Fatal("retry returned no slot")
	}
}

func TestLinearizableQueryUsesReadIndexWithoutConsumingSlots(t *testing.T) {
	wal, err := qlog.Open(t.TempDir() + "/qlog")
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()
	members := []quepaxa.Member{{ID: "n1"}}
	core := mustCore(t, "n1", members, wal, nil)
	material, err := materializer.Open(t.TempDir()+"/db.sqlite", 1)
	if err != nil {
		t.Fatal(err)
	}
	defer material.Close()
	if _, _, err := core.Propose(context.Background(), []byte("CREATE TABLE barrier_read (id INTEGER)")); err != nil {
		t.Fatal(err)
	}
	server := NewServer(core, material, "cluster", true, nil, members, 0)
	body := []byte(`{"sql":"SELECT COUNT(*) FROM barrier_read","consistency":"linearizable"}`)
	res := httptest.NewRecorder()
	server.ServeHTTP(res, httptest.NewRequest(http.MethodPost, "/sql/query", bytes.NewReader(body)))
	if res.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", res.Code, res.Body.String())
	}
	if material.Tip() != 1 {
		t.Fatalf("material tip=%d, want write at 1", material.Tip())
	}
	res = httptest.NewRecorder()
	server.ServeHTTP(res, httptest.NewRequest(http.MethodPost, "/sql/query", bytes.NewReader(body)))
	if res.Code != http.StatusOK || material.Tip() != 1 {
		t.Fatalf("second read consumed a consensus slot: status=%d tip=%d", res.Code, material.Tip())
	}

	res = httptest.NewRecorder()
	server.ServeHTTP(res, httptest.NewRequest(http.MethodPost, "/sql/query", bytes.NewReader([]byte(`{"sql":"SELECT 1","consistency":"eventual"}`))))
	if res.Code != http.StatusBadRequest {
		t.Fatalf("invalid consistency status=%d, want 400", res.Code)
	}
}

func TestLinearizableQueryFailsClosedWithoutQuorum(t *testing.T) {
	wal, err := qlog.Open(t.TempDir() + "/qlog")
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()
	members := []quepaxa.Member{{ID: "n1"}, {ID: "n2"}, {ID: "n3"}}
	core := mustCore(t, "n1", members, wal, nil)
	material, err := materializer.Open(t.TempDir()+"/db.sqlite", 1)
	if err != nil {
		t.Fatal(err)
	}
	defer material.Close()
	server := NewServer(core, material, "cluster", true, nil, members, 0)

	for consistency, want := range map[string]int{"local": http.StatusOK, "linearizable": http.StatusServiceUnavailable} {
		body := []byte(`{"sql":"SELECT 1","consistency":"` + consistency + `"}`)
		res := httptest.NewRecorder()
		server.ServeHTTP(res, httptest.NewRequest(http.MethodPost, "/sql/query", bytes.NewReader(body)))
		if res.Code != want {
			t.Fatalf("%s read status=%d, want %d: %s", consistency, res.Code, want, res.Body.String())
		}
	}
}

func TestReadyAllowsIntentionalCommitApplyGap(t *testing.T) {
	wal, err := qlog.Open(t.TempDir() + "/qlog")
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()
	members := []quepaxa.Member{{ID: "n1"}}
	core := mustCore(t, "n1", members, wal, nil)
	material, err := materializer.Open(t.TempDir()+"/db.sqlite", 1)
	if err != nil {
		t.Fatal(err)
	}
	defer material.Close()
	if _, _, err := core.Propose(context.Background(), []byte("CREATE TABLE pending (id INTEGER)")); err != nil {
		t.Fatal(err)
	}
	server := NewServer(core, material, "cluster", true, nil, members, 0, func() bool { return true })
	res := httptest.NewRecorder()
	server.ServeHTTP(res, httptest.NewRequest(http.MethodGet, "/ready", nil))
	if res.Code != http.StatusOK {
		t.Fatalf("ready status=%d body=%s", res.Code, res.Body.String())
	}
}

func TestConcurrentApplyDecisionsRemainOrdered(t *testing.T) {
	wal, err := qlog.Open(t.TempDir() + "/qlog")
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()
	members := []quepaxa.Member{{ID: "n1"}}
	core := mustCore(t, "n1", members, wal, nil)
	material, err := materializer.Open(t.TempDir()+"/db.sqlite", 1)
	if err != nil {
		t.Fatal(err)
	}
	defer material.Close()
	if _, _, err := core.Propose(context.Background(), []byte("CREATE TABLE concurrent_apply (id INTEGER)")); err != nil {
		t.Fatal(err)
	}
	for i := 1; i < 64; i++ {
		if _, _, err := core.Propose(context.Background(), []byte("INSERT INTO concurrent_apply VALUES (1)")); err != nil {
			t.Fatal(err)
		}
	}
	server := NewServer(core, material, "cluster", true, nil, members, 0)

	start := make(chan struct{})
	errs := make(chan error, 32)
	var wg sync.WaitGroup
	for i := 0; i < cap(errs); i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			errs <- server.applyDecisions(context.Background(), 64)
		}()
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	if material.Tip() != 64 {
		t.Fatalf("material tip=%d, want 64", material.Tip())
	}
}

func TestFallbackWinnerBecomesNextRequestFirstWithoutChangingAgreedLeader(t *testing.T) {
	members := []quepaxa.Member{{ID: "n1"}, {ID: "n2"}, {ID: "n3"}}
	core := mustCore(t, "n1", members, nil, nil)
	server := NewServer(core, nil, "cluster", true, nil, members, 20*time.Millisecond)
	first := server.proposerPlan()
	server.observeProposer(first, "n2", 1)
	second := server.proposerPlan()
	got := []quepaxa.NodeID{second.members[0].ID, second.members[1].ID, second.members[2].ID}
	if want := []quepaxa.NodeID{"n2", "n1", "n3"}; !slices.Equal(got, want) {
		t.Fatalf("order=%v, want %v", got, want)
	}
	server.observeProposer(first, "n3", 2)
	if got := server.proposerPlan().members[0].ID; got != "n2" {
		t.Fatalf("stale observation reverted first proposer to %s", got)
	}
	if agreed := core.ProposerOrder()[0]; agreed != "n1" {
		t.Fatalf("routing hint changed agreed fast-path leader to %s", agreed)
	}
}

func TestSQLAndKVAPIEndToEnd(t *testing.T) {
	wal, err := qlog.Open(t.TempDir() + "/qlog")
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()
	members := []quepaxa.Member{{ID: "n1"}}
	core := mustCore(t, "n1", members, wal, nil)
	material, err := materializer.Open(t.TempDir()+"/db.sqlite", 1)
	if err != nil {
		t.Fatal(err)
	}
	defer material.Close()
	server := NewServer(core, material, "cluster", true, nil, members, 0)
	request := func(path, body string) *httptest.ResponseRecorder {
		res := httptest.NewRecorder()
		server.ServeHTTP(res, httptest.NewRequest(http.MethodPost, path, bytes.NewBufferString(body)))
		return res
	}

	res := request("/sql/transaction", `{"request_id":"sql-1","statements":[{"sql":"CREATE TABLE api (id INTEGER PRIMARY KEY, name TEXT)"},{"sql":"INSERT INTO api(name) VALUES (?) RETURNING id, name","args":["bound"],"want_rows":true}]}`)
	if res.Code != http.StatusOK || !bytes.Contains(res.Body.Bytes(), []byte(`"bound"`)) {
		t.Fatalf("SQL status=%d body=%s", res.Code, res.Body.String())
	}
	res = request("/sql/query", `{"sql":"SELECT name FROM api WHERE id = ?","args":[1],"consistency":"local"}`)
	if res.Code != http.StatusOK || !bytes.Contains(res.Body.Bytes(), []byte(`"bound"`)) {
		t.Fatalf("query status=%d body=%s", res.Code, res.Body.String())
	}

	res = request("/kv/put", `{"request_id":"kv-1","key":"key","value":"dmFsdWU="}`)
	if res.Code != http.StatusOK {
		t.Fatalf("put status=%d body=%s", res.Code, res.Body.String())
	}
	res = request("/kv/get", `{"key":"key","consistency":"linearizable"}`)
	if res.Code != http.StatusOK || !bytes.Contains(res.Body.Bytes(), []byte(`"dmFsdWU="`)) {
		t.Fatalf("get status=%d body=%s", res.Code, res.Body.String())
	}
	res = request("/kv/cas", `{"request_id":"kv-2","key":"key","expected":"dmFsdWU=","expected_exists":true,"value":"bmV3"}`)
	if res.Code != http.StatusOK || !bytes.Contains(res.Body.Bytes(), []byte(`"applied":true`)) {
		t.Fatalf("cas status=%d body=%s", res.Code, res.Body.String())
	}
	res = request("/kv/put", `{"request_id":"kv-3","key":"short","value":"eA==","ttl_ms":1}`)
	if res.Code != http.StatusOK {
		t.Fatalf("TTL put status=%d body=%s", res.Code, res.Body.String())
	}
	time.Sleep(2 * time.Millisecond)
	res = request("/kv/get", `{"key":"short"}`)
	if res.Code != http.StatusOK || !bytes.Contains(res.Body.Bytes(), []byte(`"found":false`)) {
		t.Fatalf("TTL get status=%d body=%s", res.Code, res.Body.String())
	}
}

func TestKVRetryPreservesFirstAdmissionAndRejectsChangedIntent(t *testing.T) {
	members := []quepaxa.Member{{ID: "n1"}}
	core := mustCore(t, "n1", members, nil, nil)
	material, err := materializer.Open(t.TempDir()+"/db.sqlite", 1)
	if err != nil {
		t.Fatal(err)
	}
	defer material.Close()
	server := NewServer(core, material, "cluster", true, nil, members, 0)
	defer server.Close()

	req := KVMutationRequest{RequestID: "retry", Key: "key", Value: []byte("value"), TTLMS: 60_000}
	first, err := server.KVPut(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(2 * time.Millisecond)
	second, err := server.KVPut(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if first != second {
		t.Fatalf("retry changed result: first=%+v second=%+v", first, second)
	}
	req.Value = []byte("different")
	if _, err := server.KVPut(context.Background(), req); !errors.Is(err, ErrRequestConflict) {
		t.Fatalf("changed request error=%v, want conflict", err)
	}
}

func TestNotifyRetryIsIdempotentAndChangedPayloadConflicts(t *testing.T) {
	members := []quepaxa.Member{{ID: "n1"}}
	core := mustCore(t, "n1", members, nil, nil)
	material, err := materializer.Open(t.TempDir()+"/db.sqlite", 1)
	if err != nil {
		t.Fatal(err)
	}
	defer material.Close()
	server := NewServer(core, material, "cluster", true, nil, members, 0)
	defer server.Close()
	ch, cancel, err := server.NotifySubscribe("topic")
	if err != nil {
		t.Fatal(err)
	}
	defer cancel()

	command := types.NotifyCommand{RequestID: "notify", Topic: "topic", Payload: []byte("one")}
	first, err := server.NotifyPublish(context.Background(), command)
	if err != nil {
		t.Fatal(err)
	}
	second, err := server.NotifyPublish(context.Background(), command)
	if err != nil || first != second {
		t.Fatalf("retry slot=%d want=%d err=%v", second, first, err)
	}
	if got := <-ch; !bytes.Equal(got, command.Payload) {
		t.Fatalf("payload=%q", got)
	}
	select {
	case duplicate := <-ch:
		t.Fatalf("duplicate publish=%q", duplicate)
	default:
	}
	command.Payload = []byte("two")
	if _, err := server.NotifyPublish(context.Background(), command); !errors.Is(err, ErrRequestConflict) {
		t.Fatalf("changed notification error=%v, want conflict", err)
	}
}

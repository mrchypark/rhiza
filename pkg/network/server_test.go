package network

import (
	"bytes"
	"context"
	"errors"
	"math"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/materializer"
	"github.com/mrchypark/rhiza/pkg/qlog"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

type unavailableTransport struct{}

type flushSignalWriter struct {
	header  http.Header
	flushed chan struct{}
	once    sync.Once
}

func (w *flushSignalWriter) Header() http.Header             { return w.header }
func (*flushSignalWriter) Write(payload []byte) (int, error) { return len(payload), nil }
func (*flushSignalWriter) WriteHeader(int)                   {}
func (w *flushSignalWriter) Flush()                          { w.once.Do(func() { close(w.flushed) }) }

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

func mustCore(t testing.TB, nodeID quepaxa.NodeID, members []quepaxa.Member, wal *qlog.WAL, transport quepaxa.Transport) *quepaxa.Core {
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

func TestExecuteRetryResolvesCommitUnknownAfterLearnerFailure(t *testing.T) {
	members := []quepaxa.Member{{ID: "n1"}, {ID: "n2"}, {ID: "n3"}}
	transport := &retryLearnerTransport{sendErr: errors.New("learners unavailable")}
	core := mustCore(t, "n1", members, nil, transport)
	material, err := materializer.Open(t.TempDir()+"/db.sqlite", 1)
	if err != nil {
		t.Fatal(err)
	}
	defer material.Close()
	server := NewServer(core, material, "cluster", true, nil, members, 0)
	defer server.Close()
	req := ExecuteRequest{RequestID: "schema-retry", SQL: "CREATE TABLE retried (id INTEGER)"}
	if _, err := server.Execute(context.Background(), req); !errors.Is(err, ErrCommitUnknown) {
		t.Fatalf("first proposal error=%v, want commit unknown", err)
	} else {
		var unknown *CommitUnknownError
		if !errors.As(err, &unknown) || unknown.Slot != 1 || unknown.RequestID != req.RequestID {
			t.Fatalf("commit unknown detail=%#v", unknown)
		}
	}
	transport.sendErr = nil
	response, err := server.Execute(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if response.Slot != 1 || transport.sendCalls != 2 || material.Tip() != 1 {
		t.Fatalf("slot=%d SendDecision calls=%d material tip=%d, want 1, 2, 1", response.Slot, transport.sendCalls, material.Tip())
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
		status, statusErr := server.RequestStatus(context.Background(), RequestStatusRequest{Kind: "sql", RequestID: req.RequestID})
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

func TestHTTPBodyLimitIsIndependentFromConsensusLimit(t *testing.T) {
	base := []byte(`{"sql":"SELECT 1"}`)
	for _, test := range []struct {
		name string
		size int
		ok   bool
	}{
		{name: "at limit", size: MaxRequestBodyBytes, ok: true},
		{name: "over limit", size: MaxRequestBodyBytes + 1, ok: false},
	} {
		t.Run(test.name, func(t *testing.T) {
			body := append(append([]byte(nil), base...), bytes.Repeat([]byte(" "), test.size-len(base))...)
			var request QueryRequest
			err := decodeJSON(httptest.NewRecorder(), httptest.NewRequest(http.MethodPost, "/sql/query", bytes.NewReader(body)), &request)
			if (err == nil) != test.ok {
				t.Fatalf("decode error=%v, want ok=%v", err, test.ok)
			}
		})
	}
}

func BenchmarkServerQuery(b *testing.B) {
	members := []quepaxa.Member{{ID: "n1"}}
	core := mustCore(b, "n1", members, nil, nil)
	material, err := materializer.Open(b.TempDir()+"/db.sqlite", 4)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = material.Close() })
	server := NewServer(core, material, "cluster", true, nil, members, 0)
	b.Cleanup(server.Close)
	for _, consistency := range []string{"local", "linearizable"} {
		b.Run(consistency, func(b *testing.B) {
			request := QueryRequest{SQL: "SELECT 1", Consistency: consistency}
			b.ReportAllocs()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					if _, err := server.Query(context.Background(), request); err != nil {
						b.Fatal(err)
					}
				}
			})
		})
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

func TestQuiesceTimeoutKeepsAdmissionClosedUntilDrain(t *testing.T) {
	members := []quepaxa.Member{{ID: "n1"}}
	core := mustCore(t, "n1", members, nil, nil)
	material, err := materializer.Open(t.TempDir()+"/db.sqlite", 1)
	if err != nil {
		t.Fatal(err)
	}
	defer material.Close()
	server := NewServer(core, material, "cluster", true, nil, members, 0)
	defer server.Close()

	server.proposalWG.Add(1)
	var drain sync.Once
	defer func() { drain.Do(server.proposalWG.Done) }()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if resume, err := server.Quiesce(ctx); resume != nil || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("quiesce timeout error=%v", err)
	}
	value, err := types.EncodeSQLBatch([]types.SQLCommand{{RequestID: "after-timeout", SQL: "CREATE TABLE quiesce_timeout (id INTEGER)"}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := server.proposeHedged(context.Background(), value); !errors.Is(err, ErrNotReady) {
		t.Fatalf("proposal during timed-out drain error=%v, want %v", err, ErrNotReady)
	}

	drain.Do(server.proposalWG.Done)
	deadline := time.Now().Add(time.Second)
	for {
		server.proposeMu.Lock()
		quiescing := server.quiescing
		server.proposeMu.Unlock()
		if !quiescing {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("admission stayed closed after proposal drain")
		}
		time.Sleep(time.Millisecond)
	}
	if _, err := server.proposeHedged(context.Background(), value); err != nil {
		t.Fatalf("proposal after drain error=%v", err)
	}
}

func TestCatchUpCompactionTriggersHandler(t *testing.T) {
	member := quepaxa.Member{ID: "n1", Token: "secret"}
	source := mustCore(t, member.ID, []quepaxa.Member{member}, nil, nil)
	source.SetCheckpointValidator(func(context.Context, quepaxa.CheckpointSeal) error { return nil })
	if _, _, err := source.Propose(context.Background(), []byte("state")); err != nil {
		t.Fatal(err)
	}
	prefix, ok := source.PrefixHash(1)
	if !ok {
		t.Fatal("missing checkpoint prefix")
	}
	order, following, err := source.CheckpointLeaderOrders(1)
	if err != nil {
		t.Fatal(err)
	}
	seal := quepaxa.CheckpointSeal{ConfigID: source.ConfigID(), Index: 1, RootHash: [32]byte{1}, StateHash: [32]byte{2}, PrefixHash: prefix, NextLeaderOrder: order, FollowingLeaderOrder: following}
	if err := source.PrepareCheckpoint(context.Background(), seal); err != nil {
		t.Fatal(err)
	}
	sealed, err := quepaxa.EncodeCheckpointSeal(seal)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := source.Propose(context.Background(), sealed); err != nil {
		t.Fatal(err)
	}
	if err := source.CompactThrough(1, seal.RootHash); err != nil {
		t.Fatal(err)
	}
	sourceServer := NewServer(source, nil, "cluster", true, nil, []quepaxa.Member{member}, 0)
	defer sourceServer.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	peer, err := StartPeerServer(ctx, "127.0.0.1:0", sourceServer, []quepaxa.Member{member}, "admin-secret")
	if err != nil {
		t.Fatal(err)
	}
	defer peer.Close()
	member.PeerURL = "quic://" + peer.Addr()
	target := mustCore(t, member.ID, []quepaxa.Member{member}, nil, nil)
	transport := NewTransport("cluster", member.ID, &quepaxa.Cluster{Members: []quepaxa.Member{member}}, "secret")
	defer transport.Close()
	server := NewServer(target, nil, "cluster", true, transport, []quepaxa.Member{member}, 0)
	defer server.Close()
	called := make(chan struct{}, 1)
	server.SetCompactedHandler(func() { called <- struct{}{} })
	callCtx, callCancel := context.WithTimeout(ctx, 5*time.Second)
	defer callCancel()
	if err := server.catchUpFrom(callCtx, member.ID, 1); !errors.Is(err, ErrNotReady) {
		t.Fatalf("catch-up error=%v, want %v", err, ErrNotReady)
	}
	select {
	case <-called:
	case <-time.After(time.Second):
		t.Fatal("compacted handler was not called")
	}
}

func TestAcceptFromPersistsReturnedCertifiedDecision(t *testing.T) {
	member := quepaxa.Member{ID: "n1"}
	members := []quepaxa.Member{member}
	source := mustCore(t, member.ID, members, nil, nil)
	slot, _, err := source.Propose(context.Background(), []byte("value"))
	if err != nil {
		t.Fatal(err)
	}
	decision, ok := source.CertifiedValue(slot)
	if !ok {
		t.Fatal("missing certified decision")
	}
	dir := t.TempDir() + "/target-qlog"
	wal, err := qlog.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	target := mustCore(t, member.ID, members, wal, nil)
	server := NewServer(target, nil, "cluster", true, nil, members, 0)
	if err := server.acceptFrom(context.Background(), member.ID, decision); err != nil {
		t.Fatal(err)
	}
	if target.Tip() != decision.Slot {
		t.Fatalf("tip=%d, want %d", target.Tip(), decision.Slot)
	}
	server.Close()
	if err := wal.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := qlog.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	recovered := mustCore(t, member.ID, members, reopened, nil)
	if got, ok := recovered.CertifiedValue(decision.Slot); !ok || !bytes.Equal(got.Value, decision.Value) {
		t.Fatalf("recovered decision=(%v, %q), want (%v, %q)", ok, got.Value, true, decision.Value)
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

	res := request("/sql/transaction", `{"request_id":"sql-1","statements":[{"sql":"CREATE TABLE api (id INTEGER PRIMARY KEY, name TEXT)"},{"sql":"INSERT INTO api(name) VALUES (?)","args":["bound"]}]}`)
	if res.Code != http.StatusOK || !bytes.Contains(res.Body.Bytes(), []byte(`"status":"committed"`)) {
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

func TestKVMutateRejectsInvalidOperationAndTTLOverflowBeforeConsensus(t *testing.T) {
	members := []quepaxa.Member{{ID: "n1"}}
	core := mustCore(t, "n1", members, nil, nil)
	material, err := materializer.Open(t.TempDir()+"/db.sqlite", 1)
	if err != nil {
		t.Fatal(err)
	}
	defer material.Close()
	server := NewServer(core, material, "cluster", true, nil, members, 0)
	defer server.Close()

	request := KVMutationRequest{RequestID: "invalid", Key: "key", Value: []byte("value")}
	if _, err := server.KVMutate(context.Background(), "bogus", request); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("invalid operation error=%v", err)
	}
	request.RequestID = "overflow"
	request.TTLMS = math.MaxInt64
	if _, err := server.KVPut(context.Background(), request); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("overflow TTL error=%v", err)
	}
	if core.Tip() != 0 {
		t.Fatalf("invalid requests advanced consensus tip to %d", core.Tip())
	}
	request.RequestID = "valid"
	request.TTLMS = 0
	if _, err := server.KVPut(context.Background(), request); err != nil {
		t.Fatalf("valid request after rejection: %v", err)
	}
}

func TestValidateReplicatedMutationAcceptsReadBarrier(t *testing.T) {
	var nonce [types.ReadBarrierNonceSize]byte
	nonce[0] = 1
	if err := validateReplicatedMutation(types.EncodeReadBarrier(nonce)); err != nil {
		t.Fatalf("read barrier rejected: %v", err)
	}
	if err := validateReplicatedMutation([]byte("unknown-control")); err == nil {
		t.Fatal("unknown control accepted")
	}
}

func TestCancelledKVRequestStillMaterializes(t *testing.T) {
	members := []quepaxa.Member{{ID: "n1"}}
	core := mustCore(t, "n1", members, nil, nil)
	material, err := materializer.Open(t.TempDir()+"/db.sqlite", 1)
	if err != nil {
		t.Fatal(err)
	}
	defer material.Close()
	server := NewServer(core, material, "cluster", true, nil, members, 0)
	defer server.Close()
	barrier := make(chan struct{})
	entered := make(chan struct{})
	server.SetDurabilityBarrier(func(ctx context.Context, _ quepaxa.Slot) error {
		select {
		case <-entered:
		default:
			close(entered)
		}
		select {
		case <-barrier:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})

	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		_, err := server.KVPut(ctx, KVMutationRequest{RequestID: "cancelled", Key: "key", Value: []byte("value")})
		result <- err
	}()
	<-entered
	cancel()
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatalf("caller error=%v, want canceled", err)
	}
	close(barrier)

	deadline := time.Now().Add(5 * time.Second)
	for {
		receipt, found, err := material.MutationReceipt(context.Background(), types.MutationKV, "cancelled")
		if err != nil {
			t.Fatal(err)
		}
		if found {
			if receipt.Status != types.MutationCommitted || !receipt.Applied {
				t.Fatalf("receipt=%+v", receipt)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("cancelled request was not materialized")
		}
		time.Sleep(time.Millisecond)
	}
}

func TestLocalProposalAdmissionRejectsBeyondBackgroundLimit(t *testing.T) {
	members := []quepaxa.Member{{ID: "n1"}}
	core := mustCore(t, "n1", members, nil, nil)
	material, err := materializer.Open(t.TempDir()+"/db.sqlite", 1)
	if err != nil {
		t.Fatal(err)
	}
	defer material.Close()
	server := NewServer(core, material, "cluster", true, nil, members, 0)
	defer server.Close()
	release := make(chan struct{})
	entered := make(chan struct{}, maxLocalProposals)
	server.SetDurabilityBarrier(func(ctx context.Context, _ quepaxa.Slot) error {
		entered <- struct{}{}
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})
	errs := make(chan error, maxLocalProposals)
	for i := range maxLocalProposals {
		go func(i int) {
			var nonce [types.ReadBarrierNonceSize]byte
			nonce[0] = byte(i + 1)
			_, err := server.proposeHedged(context.Background(), types.EncodeReadBarrier(nonce))
			errs <- err
		}(i)
	}
	for range maxLocalProposals {
		select {
		case <-entered:
		case <-time.After(5 * time.Second):
			t.Fatal("background operations did not reach durability barrier")
		}
	}
	if _, err := server.proposeHedged(context.Background(), types.EncodeReadBarrier([types.ReadBarrierNonceSize]byte{255})); !errors.Is(err, ErrOverloaded) {
		t.Fatalf("overflow error=%v, want ErrOverloaded", err)
	}
	close(release)
	for range maxLocalProposals {
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
	}
}

func TestPeerProposalUsesAdmissionAndSemanticValidation(t *testing.T) {
	members := []quepaxa.Member{{ID: "n1"}}
	core := mustCore(t, "n1", members, nil, nil)
	material, err := materializer.Open(t.TempDir()+"/db.sqlite", 1)
	if err != nil {
		t.Fatal(err)
	}
	defer material.Close()
	server := NewServer(core, material, "cluster", true, nil, members, 0)
	defer server.Close()
	invalid, err := types.EncodeKVCommand(types.KVCommand{RequestID: "bad", Operation: "unknown", Key: "key"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := server.proposePeer(context.Background(), "n2", invalid); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("invalid peer proposal error=%v", err)
	}
	unsafeGraph, err := types.EncodeGraphCommand(types.GraphCommand{RequestID: "unsafe", Cypher: `CREATE (:Person)-[:KNOWS]->(:Person)`})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := server.proposePeer(context.Background(), "n2", unsafeGraph); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("unsafe graph peer proposal error=%v", err)
	}
	valid, err := types.EncodeKVCommand(types.KVCommand{RequestID: "valid", Operation: "put", Key: "key", Value: []byte("value")})
	if err != nil {
		t.Fatal(err)
	}
	for range maxLocalProposals {
		server.localCap <- struct{}{}
		server.operationCap <- struct{}{}
	}
	if _, err := server.proposePeer(context.Background(), "n2", valid); err != nil {
		t.Fatalf("reserved peer capacity failed: %v", err)
	}
	for range maxLocalProposals {
		<-server.localCap
		<-server.operationCap
	}
	server.proposeMu.Lock()
	server.peerCounts["n2"] = 2
	server.proposeMu.Unlock()
	if _, err := server.proposePeer(context.Background(), "n2", valid); !errors.Is(err, ErrOverloaded) {
		t.Fatalf("per-peer admission error=%v", err)
	}
	if _, err := server.proposePeer(context.Background(), "n3", valid); err != nil {
		t.Fatalf("independent peer capacity failed: %v", err)
	}
	server.proposeMu.Lock()
	delete(server.peerCounts, "n2")
	server.proposeMu.Unlock()
	for range maxPeerProposals {
		server.peerCap <- struct{}{}
	}
	if _, err := server.proposePeer(context.Background(), "n2", valid); !errors.Is(err, ErrOverloaded) {
		t.Fatalf("peer admission error=%v", err)
	}
	for range maxPeerProposals {
		<-server.peerCap
	}
}

func TestServerCloseCancelsAndWaitsForInflightMutation(t *testing.T) {
	members := []quepaxa.Member{{ID: "n1"}}
	core := mustCore(t, "n1", members, nil, nil)
	material, err := materializer.Open(t.TempDir()+"/db.sqlite", 1)
	if err != nil {
		t.Fatal(err)
	}
	defer material.Close()
	server := NewServer(core, material, "cluster", true, nil, members, 0)
	entered := make(chan struct{})
	server.SetDurabilityBarrier(func(ctx context.Context, _ quepaxa.Slot) error {
		close(entered)
		<-ctx.Done()
		return ctx.Err()
	})
	result := make(chan error, 1)
	go func() {
		_, err := server.KVPut(context.Background(), KVMutationRequest{RequestID: "shutdown", Key: "key", Value: []byte("value")})
		result <- err
	}()
	<-entered
	closed := make(chan struct{})
	go func() {
		server.Close()
		close(closed)
	}()
	select {
	case <-closed:
	case <-time.After(5 * time.Second):
		t.Fatal("server close did not wait for and cancel mutation")
	}
	if err := <-result; !errors.Is(err, ErrNotReady) && !errors.Is(err, ErrCommitUnknown) {
		t.Fatalf("mutation error=%v, want not ready or commit unknown", err)
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
		t.Fatalf("retry receipt=%+v want=%+v err=%v", second, first, err)
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

func TestNotifyStreamStopsWhenMaterializerCloses(t *testing.T) {
	members := []quepaxa.Member{{ID: "n1"}}
	core := mustCore(t, "n1", members, nil, nil)
	material, err := materializer.Open(t.TempDir()+"/db.sqlite", 1)
	if err != nil {
		t.Fatal(err)
	}
	server := NewServer(core, material, "cluster", true, nil, members, 0)
	t.Cleanup(server.Close)

	w := &flushSignalWriter{header: make(http.Header), flushed: make(chan struct{})}
	done := make(chan struct{})
	go func() {
		server.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/notify/subscribe?topic=topic", nil))
		close(done)
	}()
	<-w.flushed
	if err := material.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("notification stream did not stop after materializer close")
	}
}

func TestReplicaStatusEndpoint(t *testing.T) {
	server := NewServer(nil, nil, "cluster", false, nil, nil, 0)
	t.Cleanup(server.Close)
	response := httptest.NewRecorder()
	server.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/replica/status", nil))
	if response.Code != http.StatusNotFound {
		t.Fatalf("voter status code=%d", response.Code)
	}
	server.SetReplicaStatus(func() ReplicaStatus {
		return ReplicaStatus{Mode: "learner", AppliedSlot: 7, SourceTip: 9, LagSlots: 2, Source: "peer:n2"}
	})
	response = httptest.NewRecorder()
	server.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/replica/status", nil))
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), `"lag_slots":2`) {
		t.Fatalf("replica status code=%d body=%s", response.Code, response.Body.String())
	}
}

//go:build cgo

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/mrchypark/rhiza"
)

func openTestDB(t *testing.T) uint64 {
	t.Helper()
	return openConfig(t, rhiza.Config{NodeID: "ffi-test", DataDir: t.TempDir()})
}

func openConfig(t *testing.T, config rhiza.Config) uint64 {
	t.Helper()
	input, err := json.Marshal(config)
	if err != nil {
		t.Fatal(err)
	}
	var response struct {
		Data struct {
			Handle uint64 `json:"handle"`
		} `json:"data"`
		Error *ffiError `json:"error"`
	}
	if err := json.Unmarshal(goOpen(input), &response); err != nil {
		t.Fatal(err)
	}
	if response.Error != nil || response.Data.Handle == 0 {
		t.Fatalf("open response: %#v", response)
	}
	t.Cleanup(func() { _ = goClose(response.Data.Handle) })
	return response.Data.Handle
}

func freeUDPAddr(t testing.TB) string {
	t.Helper()
	listener, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := listener.LocalAddr().String()
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	return addr
}

func call(t *testing.T, handle uint64, operation string, request any) map[string]json.RawMessage {
	t.Helper()
	input, err := json.Marshal(map[string]any{"operation": operation, "request": request})
	if err != nil {
		t.Fatal(err)
	}
	var response map[string]json.RawMessage
	if err := json.Unmarshal(goCall(handle, input, 0), &response); err != nil {
		t.Fatal(err)
	}
	return response
}

func requireData(t *testing.T, response map[string]json.RawMessage) json.RawMessage {
	t.Helper()
	if errData, ok := response["error"]; ok {
		t.Fatalf("ffi error: %s", errData)
	}
	data, ok := response["data"]
	if !ok {
		t.Fatalf("missing data: %#v", response)
	}
	return data
}

func responseErrorCode(t *testing.T, response []byte) string {
	t.Helper()
	var env struct {
		Error *ffiError `json:"error"`
	}
	if err := json.Unmarshal(response, &env); err != nil {
		t.Fatal(err)
	}
	if env.Error == nil {
		t.Fatalf("expected error: %s", response)
	}
	return env.Error.Code
}

func TestGoEntryBasicPublicAPI(t *testing.T) {
	h := openTestDB(t)
	requireData(t, call(t, h, "execute", map[string]any{"request_id": "schema", "sql": "CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)"}))
	requireData(t, call(t, h, "execute", map[string]any{"request_id": "insert", "sql": "INSERT INTO items VALUES (?, ?)", "args": []any{int64(9007199254740993), "tea"}}))
	query := requireData(t, call(t, h, "query", map[string]any{"sql": "SELECT name FROM items WHERE id = ?", "args": []any{json.Number("9007199254740993")}}))
	if string(query) == "null" {
		t.Fatal("query returned null")
	}
	requireData(t, call(t, h, "kv_put", map[string]any{"request_id": "kv", "key": "key", "value": "dmFsdWU="}))
	kv := requireData(t, call(t, h, "kv_get", map[string]any{"key": "key"}))
	if !bytes.Contains(kv, []byte("dmFsdWU=")) {
		t.Fatalf("KV response: %s", kv)
	}
	requireData(t, call(t, h, "graph_execute", map[string]any{"request_id": "graph", "cypher": "CREATE (:Item {name: 'graph'})"}))
	graph := requireData(t, call(t, h, "graph_query", map[string]any{"cypher": "MATCH (n:Item) RETURN n.name"}))
	if !bytes.Contains(graph, []byte("graph")) {
		t.Fatalf("graph response: %s", graph)
	}
	ready := requireData(t, call(t, h, "ready", map[string]any{}))
	if string(ready) != "true" {
		t.Fatalf("ready: %s", ready)
	}
}

func TestHandleLifetimeAndInputValidation(t *testing.T) {
	h := openTestDB(t)
	if code := responseErrorCode(t, goCall(h, []byte(`{"operation":"ready","request":{}} trailing`), 0)); code != "invalid_request" {
		t.Fatalf("trailing code=%q", code)
	}
	if code := responseErrorCode(t, goCall(h, make([]byte, maxInputBytes+1), 0)); code != "invalid_request" {
		t.Fatalf("oversize code=%q", code)
	}
	if code := responseErrorCode(t, goCall(h, []byte(`{"operation":"ready","request":null}`), 0)); code != "invalid_request" {
		t.Fatalf("null code=%q", code)
	}
	if code := responseErrorCode(t, goCall(h, []byte(`{"operation":"ready","request":{}}`), math.MaxUint64)); code != "invalid_request" {
		t.Fatalf("overflow code=%q", code)
	}
	streamCall := []byte(`{"operation":"graph_stream_read","request":{"stream":"empty","wait_ms":1000}}`)
	if code := responseErrorCode(t, goCall(h, streamCall, 1)); code != "timeout" {
		t.Fatalf("long-poll timeout code=%q", code)
	}
	var closeOK map[string]json.RawMessage
	if err := json.Unmarshal(goClose(h), &closeOK); err != nil || string(closeOK["data"]) != "null" {
		t.Fatalf("close response: %#v err=%v", closeOK, err)
	}
	if code := responseErrorCode(t, goClose(h)); code != "invalid_handle" {
		t.Fatalf("double close=%q", code)
	}
	if code := responseErrorCode(t, goCall(h, []byte(`{"operation":"ready","request":{}}`), 0)); code != "invalid_handle" {
		t.Fatalf("stale call=%q", code)
	}
}

func TestOperatorListenerAndEnvOpen(t *testing.T) {
	t.Setenv("RHIZA_DATA_DIR", t.TempDir())
	t.Setenv("RHIZA_NODE_ID", "ffi-env")
	t.Setenv("RHIZA_BIND_ADDR", "127.0.0.1:0")
	t.Setenv("RHIZA_PEER_ADDR", "127.0.0.1:0")
	t.Setenv("RHIZA_CLUSTER_MEMBERS", "")
	var opened struct {
		Data struct {
			Handle uint64 `json:"handle"`
		} `json:"data"`
		Error *ffiError `json:"error"`
	}
	if err := json.Unmarshal(goOpenFromEnv(), &opened); err != nil || opened.Error != nil || opened.Data.Handle == 0 {
		t.Fatalf("open from env: %#v err=%v", opened, err)
	}
	h := opened.Data.Handle
	response := goStartOperator(h, []byte("127.0.0.1:0"))
	var started struct {
		Data struct {
			Address string `json:"address"`
		} `json:"data"`
	}
	if err := json.Unmarshal(response, &started); err != nil || started.Data.Address == "" {
		t.Fatalf("start operator: %s err=%v", response, err)
	}
	if status, err := http.Get("http://" + started.Data.Address + "/recovery/status"); err != nil || status.StatusCode != http.StatusOK {
		if status != nil {
			status.Body.Close()
		}
		t.Fatalf("recovery status: status=%v err=%v", status, err)
	} else {
		status.Body.Close()
	}
	if status, err := http.Get("http://" + started.Data.Address + "/sql/query"); err != nil || status.StatusCode != http.StatusNotFound {
		if status != nil {
			status.Body.Close()
		}
		t.Fatalf("application endpoint: status=%v err=%v", status, err)
	} else {
		status.Body.Close()
	}
	if code := responseErrorCode(t, goStartOperator(h, []byte("127.0.0.1:0"))); code != "invalid_request" {
		t.Fatalf("duplicate start code=%q", code)
	}
	if response := goClose(h); string(response) != `{"data":null}` {
		t.Fatalf("close=%s", response)
	}
	if conn, err := net.DialTimeout("tcp", started.Data.Address, 100*time.Millisecond); err == nil {
		conn.Close()
		t.Fatal("operator listener remains open after DB close")
	}
}

func TestConcurrentCallAndClose(t *testing.T) {
	h := openTestDB(t)
	entry, release, err := acquire(h)
	if err != nil {
		t.Fatal(err)
	}
	closed := make(chan []byte, 1)
	go func() { closed <- goClose(h) }()
	select {
	case <-entry.ctx.Done():
	case <-time.After(time.Second):
		t.Fatal("close did not cancel active call")
	}
	select {
	case result := <-closed:
		t.Fatalf("close completed before active call drained: %s", result)
	default:
	}
	release()
	select {
	case result := <-closed:
		var response map[string]json.RawMessage
		if err := json.Unmarshal(result, &response); err != nil || string(response["data"]) != "null" {
			t.Fatalf("close=%s err=%v", result, err)
		}
	case <-time.After(time.Second):
		t.Fatal("close did not drain")
	}
	if code := responseErrorCode(t, goCall(h, []byte(`{"operation":"ready","request":{}}`), 0)); code != "invalid_handle" {
		t.Fatalf("stale code=%q", code)
	}
}

func TestNotificationFFI(t *testing.T) {
	h := openTestDB(t)
	subscribe := requireData(t, call(t, h, "notify_subscribe", map[string]any{"topic": "jobs"}))
	var subscription struct {
		ID uint64 `json:"subscription_id"`
	}
	if err := json.Unmarshal(subscribe, &subscription); err != nil || subscription.ID == 0 {
		t.Fatalf("subscribe=%s err=%v", subscribe, err)
	}
	other := openTestDB(t)
	if code := responseErrorCode(t, goCall(other, mustCall(t, "notify_recv", map[string]any{"subscription_id": subscription.ID}), 1)); code != "invalid_subscription" {
		t.Fatalf("cross-handle receive code=%q", code)
	}
	payload := []byte{0, 255, 'o', 'k'}
	first := requireData(t, call(t, h, "notify_publish", map[string]any{"request_id": "notice", "topic": "jobs", "payload": payload}))
	second := requireData(t, call(t, h, "notify_publish", map[string]any{"request_id": "notice", "topic": "jobs", "payload": payload}))
	if !bytes.Equal(first, second) {
		t.Fatalf("retry receipt differs: first=%s second=%s", first, second)
	}
	received := requireData(t, call(t, h, "notify_recv", map[string]any{"subscription_id": subscription.ID}))
	var notice struct {
		Payload []byte `json:"payload"`
	}
	if err := json.Unmarshal(received, &notice); err != nil || !bytes.Equal(notice.Payload, payload) {
		t.Fatalf("received=%s payload=%v err=%v", received, notice.Payload, err)
	}
	if code := responseErrorCode(t, goCall(h, mustCall(t, "notify_recv", map[string]any{"subscription_id": subscription.ID}), 1)); code != "timeout" {
		t.Fatalf("empty receive code=%q", code)
	}
	requireData(t, call(t, h, "notify_unsubscribe", map[string]any{"subscription_id": subscription.ID}))
	if code := responseErrorCode(t, goCall(h, mustCall(t, "notify_recv", map[string]any{"subscription_id": subscription.ID}), 1)); code != "invalid_subscription" {
		t.Fatalf("unsubscribed receive code=%q", code)
	}
	racing := requireData(t, call(t, h, "notify_subscribe", map[string]any{"topic": "race"}))
	if err := json.Unmarshal(racing, &subscription); err != nil {
		t.Fatal(err)
	}
	requireData(t, call(t, h, "notify_publish", map[string]any{"request_id": "race", "topic": "race", "payload": []byte("queued")}))
	start := make(chan struct{})
	recvResult := make(chan []byte, 1)
	unsubscribeResult := make(chan []byte, 1)
	racingRecv := mustCall(t, "notify_recv", map[string]any{"subscription_id": subscription.ID})
	racingUnsubscribe := mustCall(t, "notify_unsubscribe", map[string]any{"subscription_id": subscription.ID})
	go func() {
		<-start
		recvResult <- goCall(h, racingRecv, 1_000)
	}()
	go func() {
		<-start
		unsubscribeResult <- goCall(h, racingUnsubscribe, 1_000)
	}()
	close(start)
	if data := <-unsubscribeResult; responseErrorCodeOrData(t, data) != "data" {
		t.Fatalf("concurrent unsubscribe=%s", data)
	}
	if result := <-recvResult; responseErrorCodeOrData(t, result) == "data" {
		var notice struct {
			Payload []byte `json:"payload"`
		}
		if err := json.Unmarshal(requireData(t, mustResponse(t, result)), &notice); err != nil || !bytes.Equal(notice.Payload, []byte("queued")) {
			t.Fatalf("concurrent receive=%s err=%v", result, err)
		}
	} else if code := responseErrorCode(t, result); code != "canceled" && code != "invalid_subscription" {
		t.Fatalf("concurrent receive code=%q", code)
	}
	if data := requireData(t, call(t, h, "notify_subscribe", map[string]any{"topic": "race"})); len(data) == 0 {
		t.Fatal("subscription capacity was not reusable after concurrent unsubscribe")
	}

	closing := requireData(t, call(t, h, "notify_subscribe", map[string]any{"topic": "closing"}))
	if err := json.Unmarshal(closing, &subscription); err != nil {
		t.Fatal(err)
	}
	entry, release, err := acquire(h)
	if err != nil {
		t.Fatal(err)
	}
	entry.mu.Lock()
	sub := entry.subscriptions[subscription.ID]
	entry.mu.Unlock()
	release()
	if sub == nil {
		t.Fatal("subscription missing before close")
	}
	recvRequest := mustCall(t, "notify_recv", map[string]any{"subscription_id": subscription.ID})
	recv := make(chan []byte, 1)
	go func() {
		recv <- goCall(h, recvRequest, 30_000)
	}()
	deadline := time.Now().Add(time.Second)
	for {
		entry.mu.Lock()
		active := entry.active
		entry.mu.Unlock()
		if active > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("receive did not become active")
		}
		time.Sleep(time.Millisecond)
	}
	if response := goClose(h); string(response) != `{"data":null}` {
		t.Fatalf("close=%s", response)
	}
	if code := responseErrorCode(t, <-recv); code != "canceled" {
		t.Fatalf("close receive code=%q", code)
	}
	select {
	case <-sub.ctx.Done():
	case <-time.After(time.Second):
		t.Fatal("close did not cancel subscription")
	}
}

func TestNotificationFFISubscriptionLimitAndReuse(t *testing.T) {
	h := openTestDB(t)
	ids := make([]uint64, 0, maxSubscriptions)
	for range maxSubscriptions {
		data := requireData(t, call(t, h, "notify_subscribe", map[string]any{"topic": "jobs"}))
		var response struct {
			ID uint64 `json:"subscription_id"`
		}
		if err := json.Unmarshal(data, &response); err != nil || response.ID == 0 {
			t.Fatalf("subscribe=%s err=%v", data, err)
		}
		ids = append(ids, response.ID)
	}
	if code := responseErrorCode(t, goCall(h, mustCall(t, "notify_subscribe", map[string]any{"topic": "jobs"}), 0)); code != "overloaded" {
		t.Fatalf("subscription limit code=%q", code)
	}
	requireData(t, call(t, h, "notify_unsubscribe", map[string]any{"subscription_id": ids[0]}))
	data := requireData(t, call(t, h, "notify_subscribe", map[string]any{"topic": "jobs"}))
	var replacement struct {
		ID uint64 `json:"subscription_id"`
	}
	if err := json.Unmarshal(data, &replacement); err != nil || replacement.ID == 0 {
		t.Fatalf("replacement=%s err=%v", data, err)
	}
}

func TestNotificationFFIThreeNode(t *testing.T) {
	endpoint := os.Getenv("RHIZA_E2E_S3_ENDPOINT")
	bucket := os.Getenv("RHIZA_E2E_S3_BUCKET")
	if endpoint == "" || bucket == "" {
		t.Skip("RHIZA_E2E_S3_ENDPOINT and RHIZA_E2E_S3_BUCKET are required")
	}
	addresses := []string{freeUDPAddr(t), freeUDPAddr(t), freeUDPAddr(t)}
	members := make([]rhiza.Member, len(addresses))
	for i, address := range addresses {
		members[i] = rhiza.Member{ID: rhiza.NodeID(fmt.Sprintf("n%d", i+1)), PeerURL: "quic://" + address, Token: fmt.Sprintf("voter-token-%d", i+1)}
	}
	cluster := fmt.Sprintf("ffi-notify-%d", time.Now().UnixNano())
	config := func(index int) rhiza.Config {
		return rhiza.Config{
			ClusterID: cluster, NodeID: string(members[index].ID), DataDir: t.TempDir(), PeerAddr: addresses[index], Members: members,
			ObjStoreProvider: "s3", ObjStoreEndpoint: endpoint, ObjStoreBucket: bucket, ObjStoreRegion: "us-east-1", ObjStoreInsecure: true,
			ObjStoreAccessKey: os.Getenv("RHIZA_E2E_S3_ACCESS_KEY"), ObjStoreSecretKey: os.Getenv("RHIZA_E2E_S3_SECRET_KEY"),
		}
	}
	hA := openConfig(t, config(0))
	hB := openConfig(t, config(1))
	_ = openConfig(t, config(2))

	subscribe := requireData(t, call(t, hB, "notify_subscribe", map[string]any{"topic": "jobs"}))
	var subscription struct {
		ID uint64 `json:"subscription_id"`
	}
	if err := json.Unmarshal(subscribe, &subscription); err != nil || subscription.ID == 0 {
		t.Fatalf("subscribe=%s err=%v", subscribe, err)
	}
	request := map[string]any{"request_id": "cross-node", "topic": "jobs", "payload": []byte{0, 255, 'o', 'k'}}
	publishRequest := mustCall(t, "notify_publish", request)
	deadline := time.Now().Add(10 * time.Second)
	var first json.RawMessage
	for time.Now().Before(deadline) {
		var response map[string]json.RawMessage
		if err := json.Unmarshal(goCall(hA, publishRequest, 500), &response); err != nil {
			t.Fatal(err)
		}
		if data, ok := response["data"]; ok {
			first = data
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if first == nil {
		t.Fatal("cross-node publish did not reach quorum")
	}
	var notice struct {
		Payload []byte `json:"payload"`
	}
	data := requireData(t, call(t, hB, "notify_recv", map[string]any{"subscription_id": subscription.ID}))
	if err := json.Unmarshal(data, &notice); err != nil || !bytes.Equal(notice.Payload, []byte{0, 255, 'o', 'k'}) {
		t.Fatalf("received=%s payload=%v err=%v", data, notice.Payload, err)
	}
	if second := requireData(t, call(t, hA, "notify_publish", request)); !bytes.Equal(first, second) {
		t.Fatalf("duplicate receipt differs: first=%s second=%s", first, second)
	}
	if code := responseErrorCode(t, goCall(hB, mustCall(t, "notify_recv", map[string]any{"subscription_id": subscription.ID}), 50)); code != "timeout" {
		t.Fatalf("duplicate delivery code=%q", code)
	}
}

func mustCall(t *testing.T, operation string, request any) []byte {
	t.Helper()
	data, err := json.Marshal(map[string]any{"operation": operation, "request": request})
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func responseErrorCodeOrData(t *testing.T, response []byte) string {
	t.Helper()
	var envelope map[string]json.RawMessage
	if err := json.Unmarshal(response, &envelope); err != nil {
		t.Fatal(err)
	}
	if _, ok := envelope["data"]; ok {
		return "data"
	}
	return responseErrorCode(t, response)
}

func mustResponse(t *testing.T, response []byte) map[string]json.RawMessage {
	t.Helper()
	var envelope map[string]json.RawMessage
	if err := json.Unmarshal(response, &envelope); err != nil {
		t.Fatal(err)
	}
	return envelope
}

func TestErrorJSONAndCommitUnknownPriority(t *testing.T) {
	if !json.Valid(marshalError("bad\x00code", "line\n\x01")) {
		t.Fatalf("error JSON is invalid: %q", marshalError("bad\x00code", "line\n\x01"))
	}
	if code := errorCode(fmt.Errorf("%w while %w", rhiza.ErrCommitUnknown, context.Canceled)); code != "commit_unknown" {
		t.Fatalf("commit unknown code=%q", code)
	}
}

func Example_build() { fmt.Println("go build -buildmode=c-archive -o rhiza_ffi.a ./cmd/rhiza-ffi") }

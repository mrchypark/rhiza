package managedsql

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	objectstore "github.com/mrchypark/rhiza/internal/objstore"
	thanosobjstore "github.com/thanos-io/objstore"
)

func TestSQLWireStoreInterruptedPutAndSDKControls(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	server, control := sqlWireStore(t)
	control.finalPrefix = "test-managed-graph"
	control.putReadErrors = make(chan error, 2)
	control.putReady = make(chan string, 1)
	control.putRelease = make(chan struct{})
	control.putPrefix = "/request-cancel/"
	control.putSuffix = ".block"
	control.putReadGateBytes = 1
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(control.putRelease) }) }
	t.Cleanup(release)
	cfg := objectstore.Config{Provider: objectstore.ProviderS3, Endpoint: strings.TrimPrefix(server.URL, "http://"), Bucket: "bucket", Region: "us-east-1", Insecure: true, AccessKey: "test", SecretKey: "test"}
	bucket, err := objectstore.NewBucketForNamespace(ctx, cfg, control.finalPrefix)
	if err != nil {
		t.Fatal(err)
	}
	defer bucket.Close()
	readObject := func(key string) ([]byte, bool) {
		t.Helper()
		r, err := bucket.Get(ctx, key)
		if err != nil {
			if bucket.IsObjNotFoundErr(err) {
				return nil, false
			}
			t.Fatalf("SDK GET %q: %v", key, err)
		}
		body, readErr := io.ReadAll(r)
		if err := errors.Join(readErr, r.Close()); err != nil {
			t.Fatalf("read SDK object %q: %v", key, err)
		}
		return body, true
	}

	// The positive control uses the real configured S3 SDK and checks exact
	// bytes. Repeating the same key with If-None-Match must return typed 412.
	positiveKey := control.finalPrefix + "/checkpoint/blocks/positive.block"
	positive := []byte("complete sdk object")
	if err := bucket.Upload(ctx, positiveKey, bytes.NewReader(positive), thanosobjstore.WithIfNotExists()); err != nil {
		t.Fatalf("normal SDK PUT: %v", err)
	}
	if got, ok := readObject(positiveKey); !ok || !bytes.Equal(got, positive) {
		t.Fatalf("normal SDK PUT readback=(%q,%t), want (%q,true)", got, ok, positive)
	}
	if err := bucket.Upload(ctx, positiveKey, bytes.NewReader([]byte("must-not-replace")), thanosobjstore.WithIfNotExists()); err == nil || !bucket.IsConditionNotMetErr(err) {
		t.Fatalf("existing-object conditional SDK PUT error=%v; want typed condition conflict", err)
	}
	if got, ok := readObject(positiveKey); !ok || !bytes.Equal(got, positive) {
		t.Fatalf("412 changed existing object: readback=(%q,%t)", got, ok)
	}

	// Hold an actual SDK PUT before the mock consumes its body. Caller
	// cancellation must return to the SDK caller; after releasing the existing
	// gate, the mock reads the real interrupted request body and stores nothing.
	// The reader's remaining-byte counter makes this a proven partial upload,
	// rather than relying on a payload size to outrun local buffering.
	cancelKey := control.finalPrefix + "/request-cancel/caller.block"
	callerCtx, cancelCaller := context.WithCancel(ctx)
	callerBody := newObservedSDKUploadReader(bytes.Repeat([]byte("c"), 4<<20))
	callerResult := make(chan error, 1)
	go func() {
		callerResult <- bucket.Upload(callerCtx, cancelKey, callerBody)
	}()
	select {
	case path := <-control.putReady:
		if !strings.HasSuffix(path, "/request-cancel/caller.block") {
			t.Fatalf("caller-cancel gate captured wrong PUT: %s", path)
		}
	case <-ctx.Done():
		cancelCaller()
		t.Fatalf("SDK PUT did not reach caller-cancel gate: %v", ctx.Err())
	}
	if remaining := callerBody.remaining.Load(); remaining == 0 {
		cancelCaller()
		release()
		t.Fatalf("SDK source was exhausted before server reached the partial-body gate")
	}
	cancelCaller()
	select {
	case err := <-callerResult:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("active SDK caller cancellation error=%v; want context.Canceled", err)
		}
	case <-ctx.Done():
		t.Fatalf("SDK caller did not receive cancellation: %v", ctx.Err())
	}
	release()
	readErr := receiveReadError(t, ctx, control.putReadErrors)
	if !strings.HasPrefix(readErr.Error(), "/bucket/"+cancelKey+":") || !strings.Contains(strings.ToLower(readErr.Error()), "unexpected eof") {
		t.Fatalf("caller-canceled SDK body read error=%v; want its actual unexpected EOF", readErr)
	}
	if _, ok := readObject(cancelKey); ok {
		t.Fatal("caller-canceled SDK PUT stored an object")
	}

	// Send a live malformed request over TCP: declare more bytes than are sent,
	// then half-close the client write side. This exercises the mock's actual
	// io.ReadAll error branch, not a synthetic reader or Host.Close attribution.
	oldKey := control.finalPrefix + "/checkpoint/blocks/keep-old.block"
	oldBody := []byte("prior complete object")
	if err := bucket.Upload(ctx, oldKey, bytes.NewReader(oldBody), thanosobjstore.WithIfNotExists()); err != nil {
		t.Fatalf("seed existing object with SDK: %v", err)
	}
	status, responseBody := sendTruncatedPut(t, ctx, server, "/bucket/"+oldKey, []byte("short"), 96)
	readErr = receiveReadError(t, ctx, control.putReadErrors)
	if !strings.Contains(strings.ToLower(readErr.Error()), "unexpected eof") {
		t.Fatalf("mock ReadAll error=%v; want unexpected EOF", readErr)
	}
	if status != http.StatusBadRequest {
		t.Fatalf("truncated overwrite status=%d body=%q; want 400", status, responseBody)
	}
	if got, ok := readObject(oldKey); !ok || !bytes.Equal(got, oldBody) {
		t.Fatalf("truncated PUT changed existing object: readback=(%q,%t)", got, ok)
	}

	freshKey := control.finalPrefix + "/checkpoint/blocks/no-partial.block"
	status, responseBody = sendTruncatedPut(t, ctx, server, "/bucket/"+freshKey, []byte("partial"), 96)
	readErr = receiveReadError(t, ctx, control.putReadErrors)
	if !strings.Contains(strings.ToLower(readErr.Error()), "unexpected eof") {
		t.Fatalf("fresh-key mock ReadAll error=%v; want unexpected EOF", readErr)
	}
	if status != http.StatusBadRequest {
		t.Fatalf("truncated fresh-key PUT status=%d body=%q; want 400", status, responseBody)
	}
	if _, ok := readObject(freshKey); ok {
		t.Fatal("truncated fresh-key PUT stored a partial object")
	}
}

// observedSDKUploadReader exposes known size and atomic source progress for the
// real SDK. The caller can prove bytes remain unsent when the server has read
// only its gated prefix and the request is canceled.
type observedSDKUploadReader struct {
	reader    *bytes.Reader
	size      int64
	remaining atomic.Int64
}

func newObservedSDKUploadReader(data []byte) *observedSDKUploadReader {
	r := &observedSDKUploadReader{reader: bytes.NewReader(data), size: int64(len(data))}
	r.remaining.Store(r.size)
	return r
}

func (r *observedSDKUploadReader) ObjectSize() (int64, error) { return r.size, nil }

func (r *observedSDKUploadReader) Read(p []byte) (int, error) {
	n, err := r.reader.Read(p)
	r.remaining.Store(int64(r.reader.Len()))
	return n, err
}

func TestSQLWireStoreNilObserverPreservesFailureSignal(t *testing.T) {
	cmd := exec.Command(os.Args[0], "-test.run=^TestSQLWireStoreNilObserverFailureProbe$")
	cmd.Env = append(os.Environ(), "RHIZA_SQLWIRE_NIL_OBSERVER_PROBE=1")
	output, err := cmd.CombinedOutput()
	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) || exitErr.ExitCode() == 0 || !strings.Contains(string(output), "unexpected EOF") {
		t.Fatalf("nil-observer subprocess err=%v output=%s; want test failure containing unexpected EOF", err, output)
	}
	t.Logf("nil observer preserved testing.T failure signal: %s", strings.TrimSpace(string(output)))
}

func TestSQLWireStoreNilObserverFailureProbe(t *testing.T) {
	if os.Getenv("RHIZA_SQLWIRE_NIL_OBSERVER_PROBE") != "1" {
		t.Skip("subprocess-only failure probe")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	server, control := sqlWireStore(t)
	control.finalPrefix = "test-managed-graph"
	status, _ := sendTruncatedPut(t, ctx, server, "/bucket/test-managed-graph/no-observer.block", []byte("partial"), 96)
	if status != http.StatusInternalServerError {
		t.Fatalf("nil-observer incomplete PUT status=%d; want original 500", status)
	}
}

func receiveReadError(t *testing.T, ctx context.Context, errors <-chan error) error {
	t.Helper()
	select {
	case err := <-errors:
		return err
	case <-ctx.Done():
		t.Fatalf("mock did not report the actual body-read error: %v", ctx.Err())
		return nil
	}
}

func sendTruncatedPut(t *testing.T, ctx context.Context, server *httptest.Server, requestPath string, partial []byte, declaredLength int) (int, string) {
	t.Helper()
	conn, err := (&net.Dialer{}).DialContext(ctx, "tcp", server.Listener.Addr().String())
	if err != nil {
		t.Fatalf("dial fixture HTTP server: %v", err)
	}
	defer conn.Close()
	if _, err := fmt.Fprintf(conn, "PUT %s HTTP/1.1\r\nHost: fixture\r\nContent-Length: %d\r\nConnection: close\r\n\r\n", requestPath, declaredLength); err != nil {
		t.Fatalf("write truncated PUT headers: %v", err)
	}
	if _, err := conn.Write(partial); err != nil {
		t.Fatalf("write truncated PUT prefix: %v", err)
	}
	if tcp, ok := conn.(*net.TCPConn); !ok {
		t.Fatal("fixture connection is not TCP")
	} else if err := tcp.CloseWrite(); err != nil {
		t.Fatalf("half-close truncated PUT: %v", err)
	}
	response, err := http.ReadResponse(bufio.NewReader(conn), &http.Request{Method: http.MethodPut})
	if err != nil {
		t.Fatalf("read truncated PUT response: %v", err)
	}
	body, readErr := io.ReadAll(response.Body)
	if err := errors.Join(readErr, response.Body.Close()); err != nil {
		t.Fatalf("read truncated PUT response body: %v", err)
	}
	return response.StatusCode, string(body)
}

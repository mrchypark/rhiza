//go:build rhiza_local_testhooks

package managedsql

import (
	"context"
	"errors"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/localtesthooks"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/network"
)

// This tests Host mutex serialization, not Node.Shutdown cancelling an active
// borrowed operation. The latter has a separate Node lifetime regression.
func TestInternalRequestHostSerializesCloseBehindExecute(t *testing.T) {
	var checkpointPaused, archivePaused atomic.Int32
	backgroundRelease := make(chan struct{})
	restoreBackground := localtesthooks.SetBackgroundPublication(func(ctx context.Context, phase string) {
		switch phase {
		case "checkpoint":
			checkpointPaused.Add(1)
		case "archive":
			archivePaused.Add(1)
		default:
			t.Errorf("unexpected background publication phase %q", phase)
		}
		select {
		case <-ctx.Done():
		case <-backgroundRelease:
		}
	})
	// Registered before fixture cleanup: each Host must cancel and join its
	// paused workers before the hook is restored or its fallback is released.
	t.Cleanup(func() { close(backgroundRelease); restoreBackground() })
	var outage, armed atomic.Bool
	var attempts atomic.Int64
	entered := make(chan string, 8)
	readErrors := make(chan error, 8)
	release := make(chan struct{})
	var releaseOnce sync.Once
	var gatedPath string
	var canceledObserved bool
	var gateHost *Host
	// Register before the fixture: inspect every captured error only after its
	// Hosts and HTTP server have joined. Other requests/errors remain failures.
	t.Cleanup(func() {
		for {
			select {
			case err := <-readErrors:
				if !canceledObserved || gatedPath == "" || !strings.HasPrefix(err.Error(), gatedPath+": ") || !errors.Is(err, io.ErrUnexpectedEOF) {
					t.Errorf("unexpected source request read failure: %v", err)
				}
			default:
				return
			}
		}
	})
	_, hosts, _ := openInternalRequestHosts(t, &outage, &attempts, func(source *sqlWireControl) {
		source.putReady, source.putRelease = entered, release
		source.putSuffix = "archive/head.bin"
		source.putEnabled = &armed
		source.putReadErrors = readErrors
		source.putGate = func(requestCtx context.Context, _ string) bool {
			// Admission/control publication must remain free to complete
			// before this exact SQL mutation has actually been applied.
			observe, stop := context.WithTimeout(requestCtx, time.Second)
			defer stop()
			status, err := gateHost.node.RequestStatusManaged(observe, network.RequestStatusRequest{Kind: "sql", RequestID: "close-execute"})
			if err != nil {
				t.Errorf("observe exact applied receipt before archive gate: %v", err)
				return false
			}
			return status.Receipt != nil && status.Receipt.Status == types.MutationCommitted
		}
	})
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	for _, host := range hosts {
		awaitPublicInternalReady(t, ctx, host)
	}
	// Both background writers must be paused before arming the foreground
	// BeforeAck PUT. Ready alone does not join the first checkpoint publisher.
	pauseTick := time.NewTicker(10 * time.Millisecond)
	defer pauseTick.Stop()
	for checkpointPaused.Load() < int32(len(hosts)) || archivePaused.Load() < int32(len(hosts)) {
		select {
		case <-ctx.Done():
			t.Fatal("background publication workers did not pause", ctx.Err())
		case <-pauseTick.C:
		}
	}
	host := hosts[0]
	gateHost = host // published to the HTTP fixture by armed.Store below
	type execution struct {
		response ExecuteResponse
		err      error
	}
	operation := make(chan execution, 1)
	closeStarted := make(chan struct{})
	closed := make(chan error, 1)
	var callers sync.WaitGroup
	t.Cleanup(func() {
		releaseOnce.Do(func() { close(release) })
		cancel()
		callers.Wait() // release all SDK work before fixture Host/server cleanup
	})
	armed.Store(true)
	callers.Add(1)
	go func() {
		defer callers.Done()
		response, err := host.Execute(ctx, ExecuteRequest{RequestID: "close-execute", SQL: "CREATE TABLE close_execute(id INTEGER)"})
		operation <- execution{response, err}
	}()
	select {
	case gatedPath = <-entered:
		armed.Store(false) // retain this blocked request, not later shutdown PUTs
		if !strings.HasSuffix(gatedPath, "/archive/head.bin") {
			t.Fatal("Close fixture gated a non-archive request")
		}
	case result := <-operation:
		t.Fatalf("Execute completed before archive gate: %+v %v", result.response, result.err)
	case <-ctx.Done():
		t.Fatal("archive gate not reached", ctx.Err())
	}
	// Inspect the real applied receipt without trying to acquire the Host mutex.
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	for {
		status, err := host.node.RequestStatusManaged(ctx, network.RequestStatusRequest{Kind: "sql", RequestID: "close-execute"})
		if err == nil && status.Receipt != nil && status.Receipt.Status == types.MutationCommitted {
			break
		}
		select {
		case result := <-operation:
			t.Fatalf("Execute returned before applied receipt: %+v %v", result.response, result.err)
		case <-ctx.Done():
			t.Fatal("applied receipt not observed", ctx.Err())
		case <-tick.C:
		}
	}
	original, found, err := host.node.RetainedRequest(ctx, "sql", "close-execute")
	if err != nil || !found || original.Slot == 0 || original.SQL == nil || original.SQL.RequestID != "close-execute" || original.SQL.SQL != "CREATE TABLE close_execute(id INTEGER)" {
		t.Fatalf("applied exact original missing before Close cancellation: found=%t original=%+v error=%v", found, original, err)
	}
	callers.Add(1)
	go func() {
		defer callers.Done()
		close(closeStarted) // attempted public Close, not a Node shutdown marker
		closed <- host.Close()
	}()
	select {
	case <-closeStarted:
	case <-ctx.Done():
		t.Fatal("Close caller did not start", ctx.Err())
	}
	select {
	case err := <-closed:
		t.Fatalf("Close finished while real Execute held SDK gate: %v", err)
	default:
	}
	// closeStarted is only a caller-start signal. Observe the actual canceled
	// SDK operation before releasing its request body; scheduler timing must not
	// decide between a normal ACK and the committed-but-unknown outcome.
	select {
	case result := <-operation:
		var unknown *network.CommitUnknownError
		if !errors.As(result.err, &unknown) || !errors.Is(result.err, context.Canceled) || unknown.RequestID != "close-execute" || result.response.Status == types.MutationCommitted {
			t.Fatalf("Close cancellation did not preserve exact committed-but-unknown outcome: response=%+v error=%v", result.response, result.err)
		}
		canceledObserved = true
	case <-ctx.Done():
		t.Fatal("Execute did not drain", ctx.Err())
	}
	releaseOnce.Do(func() { close(release) })
	select {
	case err := <-closed:
		if err != nil {
			t.Fatal("Close after Execute", err)
		}
	case <-ctx.Done():
		t.Fatal("Close did not finish after Execute", ctx.Err())
	}
	if err := host.Ready(ctx); err == nil {
		t.Fatal("closed Host became Ready")
	}
	if _, err := host.Execute(ctx, ExecuteRequest{RequestID: "after-close", SQL: "SELECT 1"}); err == nil {
		t.Fatal("closed Host accepted new Execute")
	}
	if _, err := host.RecoverRequest(ctx, network.RequestStatusRequest{Kind: "sql", RequestID: "close-execute"}); err == nil {
		t.Fatal("closed Host accepted new recovery")
	}
	if err := host.node.CheckManagedLocalRead(ctx); err == nil {
		t.Fatal("closed Node retained an accessible runtime")
	}
}

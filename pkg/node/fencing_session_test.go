package node

import (
	"context"
	"errors"
	"net/url"
	"os"
	"path/filepath"
	"testing"

	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/fencing"
)

func TestFenceStartupPartialConfigurationRejectsBeforeCreatingState(t *testing.T) {
	for _, field := range []string{"socket", "ticket"} {
		t.Run(field, func(t *testing.T) {
			t.Setenv("RHIZA_FENCE_SOCKET", "")
			t.Setenv("RHIZA_FENCE_TICKET", "")
			if field == "socket" {
				t.Setenv("RHIZA_FENCE_SOCKET", "/does/not/exist")
			} else {
				t.Setenv("RHIZA_FENCE_TICKET", "test-ticket")
			}
			dir := filepath.Join(t.TempDir(), "unopened")
			n := New(&types.ExecutionConfig{ClusterID: "cluster", NodeID: "node", DataDir: dir})
			if err := n.Open(context.Background()); err == nil {
				t.Fatal("partial supervision accepted")
			}
			if _, err := os.Stat(dir); !os.IsNotExist(err) {
				t.Fatalf("startup gate created state: %v", err)
			}
		})
	}
}

func TestFenceWatcherRetriesOnlyPendingAndTransportUncertainty(t *testing.T) {
	for _, tc := range []struct {
		err   error
		retry bool
	}{
		{fencing.ErrPending, true},
		{&url.Error{Op: "Post", URL: "http://supervisor/v1/stop", Err: errors.New("connection refused")}, true},
		{context.DeadlineExceeded, true},
		{fencing.ErrClosed, false},
		{errors.New("fencing: supervisor returned HTTP 403"), false},
		{errors.New("fencing: stop request identity mismatch"), false},
		{nil, false},
	} {
		if got := retryFenceSession(tc.err); got != tc.retry {
			t.Fatalf("error=%v retry=%t want=%t", tc.err, got, tc.retry)
		}
	}
}

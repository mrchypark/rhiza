package network

import (
	"context"
	"errors"
	"testing"

	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/materializer"
)

func TestManagedGraphReadinessFailureClassification(t *testing.T) {
	for _, tc := range []struct {
		name   string
		server *Server
		want   string
	}{
		{"not writable", &Server{}, "not_writable"},
		{"no callback", &Server{writable: true}, "callback_missing"},
		{"callback false", &Server{writable: true, ready: func() bool { return false }}, "callback_false"},
		{"lifecycle failed", &Server{writable: true, ready: func() bool { return true }, lifecycleFailure: errors.New("secret")}, "lifecycle_failed"},
		{"no material", &Server{writable: true, ready: func() bool { return true }}, "material_missing"},
		{"ready", &Server{writable: true, ready: func() bool { return true }, material: &materializer.Materializer{}}, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.server.managedGraphReadinessFailure(); got != tc.want {
				t.Fatalf("got %q, want %q", got, tc.want)
			}
			if tc.want != "" {
				_, err := tc.server.graphExecuteManaged(context.Background(), types.GraphCommand{})
				if !errors.Is(err, ErrNotReady) {
					t.Fatalf("changed error: %v", err)
				}
			}
		})
	}
}

package network

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

// membershipCallback is a channel-controlled callback for deterministic tests.
type membershipCallback struct {
	unblock chan struct{}
	called  chan struct{}
}

func newMembershipCallback() *membershipCallback {
	return &membershipCallback{
		unblock: make(chan struct{}),
		called:  make(chan struct{}),
	}
}

// handler returns a callback that signals called when entered, then blocks on
// unblock or ctx.  Calling close(cb.unblock) lets it return nil; if the
// context is cancelled first it returns ctx.Err().
func (cb *membershipCallback) handler() func(context.Context, MembershipChange) error {
	return func(ctx context.Context, _ MembershipChange) error {
		close(cb.called)
		select {
		case <-cb.unblock:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func validChangeJSON(t *testing.T) string {
	t.Helper()
	b, err := json.Marshal(MembershipChange{
		OperationID:      "op-1",
		ClusterID:        "cluster",
		ExpectedConfigID: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func membershipPOST(t *testing.T, token, body string) *http.Request {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/membership/change", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("Authorization", token)
	}
	return req
}

func TestChangeMembershipRejectsUnauthorized(t *testing.T) {
	core := mustCore(t, "n1", []quepaxa.Member{{ID: "n1"}}, nil, nil)
	server := NewServer(core, nil, "cluster", true, nil)
	t.Cleanup(server.Close)
	server.SetMembershipToken("admin-secret")
	called := make(chan struct{})
	server.SetMembershipChange(func(context.Context, MembershipChange) error {
		close(called)
		return nil
	})
	for _, auth := range []string{"", "Bearer wrong", "Token admin-secret"} {
		w := httptest.NewRecorder()
		server.ServeHTTP(w, membershipPOST(t, auth, validChangeJSON(t)))
		if w.Code != http.StatusForbidden {
			t.Fatalf("auth=%q status=%d, want 403", auth, w.Code)
		}
	}
	select {
	case <-called:
		t.Fatal("callback invoked for unauthorized request")
	default:
	}
}

func TestChangeMembershipRejectsInvalidInput(t *testing.T) {
	core := mustCore(t, "n1", []quepaxa.Member{{ID: "n1"}}, nil, nil)
	server := NewServer(core, nil, "cluster", true, nil)
	t.Cleanup(server.Close)
	server.SetMembershipToken("tok")
	called := make(chan struct{})
	server.SetMembershipChange(func(context.Context, MembershipChange) error {
		close(called)
		return nil
	})
	valid := validChangeJSON(t)
	type badCase struct {
		name string
		req  func() *http.Request
		code int
	}
	cases := []badCase{
		{"garbage", func() *http.Request { return membershipPOST(t, "Bearer tok", "not json") }, http.StatusBadRequest},
		{"unknown_field", func() *http.Request {
			return membershipPOST(t, "Bearer tok", `{"unknown":"x","operation_id":"o","cluster_id":"c","expected_config_id":1}`)
		}, http.StatusBadRequest},
		{"trailing_bytes", func() *http.Request {
			return membershipPOST(t, "Bearer tok", valid+",{}")
		}, http.StatusBadRequest},
		{"over_64KiB", func() *http.Request {
			big := `{"operation_id":"` + strings.Repeat("x", 65<<10) + `"}`
			return membershipPOST(t, "Bearer tok", big)
		}, http.StatusBadRequest},
		{"wrong_method", func() *http.Request {
			req := httptest.NewRequest(http.MethodGet, "/membership/change", nil)
			req.Header.Set("Authorization", "Bearer tok")
			return req
		}, http.StatusMethodNotAllowed},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			w := httptest.NewRecorder()
			server.ServeHTTP(w, tc.req())
			if w.Code != tc.code {
				t.Fatalf("case=%s status=%d, want %d body=%s", tc.name, w.Code, tc.code, w.Body.String())
			}
		})
	}
	select {
	case <-called:
		t.Fatal("callback invoked for invalid input")
	default:
	}
}

func TestChangeMembershipCloseJoinsInflightCallback(t *testing.T) {
	core := mustCore(t, "n1", []quepaxa.Member{{ID: "n1"}}, nil, nil)
	server := NewServer(core, nil, "cluster", true, nil)
	entered, canceled, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
	server.SetMembershipChange(func(ctx context.Context, _ MembershipChange) error {
		close(entered)
		<-ctx.Done()
		close(canceled)
		<-release
		return ctx.Err()
	})
	done := make(chan error, 1)
	go func() { done <- server.ChangeMembership(context.Background(), MembershipChange{}) }()
	<-entered
	closeDone := make(chan struct{})
	go func() { server.Close(); close(closeDone) }()
	select {
	case <-canceled:
	case <-time.After(5 * time.Second):
		close(release)
		t.Fatal("Close did not cancel callback")
	}
	select {
	case <-closeDone:
		t.Fatal("Close returned before callback finished")
	default:
	}
	close(release)
	select {
	case <-closeDone:
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not join callback")
	}
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("callback error=%v", err)
	}
}

func TestChangeMembershipRejectsAfterClose(t *testing.T) {
	core := mustCore(t, "n1", []quepaxa.Member{{ID: "n1"}}, nil, nil)
	server := NewServer(core, nil, "cluster", true, nil)
	server.SetMembershipToken("tok")
	server.SetMembershipChange(func(context.Context, MembershipChange) error {
		t.Fatal("callback should not be called after Close")
		return nil
	})
	server.Close()
	w := httptest.NewRecorder()
	server.ServeHTTP(w, membershipPOST(t, "Bearer tok", validChangeJSON(t)))
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("after Close status=%d, want 503", w.Code)
	}
	var resp ErrorResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if resp.Code != "not_ready" {
		t.Fatalf("after Close code=%s, want not_ready", resp.Code)
	}
}

func TestChangeMembershipPropagatesCallbackError(t *testing.T) {
	core := mustCore(t, "n1", []quepaxa.Member{{ID: "n1"}}, nil, nil)
	server := NewServer(core, nil, "cluster", true, nil)
	t.Cleanup(server.Close)
	server.SetMembershipToken("tok")
	server.SetMembershipChange(func(context.Context, MembershipChange) error {
		return ErrNotReady
	})
	w := httptest.NewRecorder()
	server.ServeHTTP(w, membershipPOST(t, "Bearer tok", validChangeJSON(t)))
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("status=%d, want 503", w.Code)
	}
}

func TestChangeMembershipCallbackReceivesDecodedChange(t *testing.T) {
	core := mustCore(t, "n1", []quepaxa.Member{{ID: "n1"}}, nil, nil)
	server := NewServer(core, nil, "cluster", true, nil)
	t.Cleanup(server.Close)
	server.SetMembershipToken("tok")
	got := make(chan MembershipChange, 1)
	server.SetMembershipChange(func(_ context.Context, change MembershipChange) error {
		got <- change
		return nil
	})
	body := `{"operation_id":"op-99","cluster_id":"cl","expected_config_id":3,"remove":"n2"}`
	w := httptest.NewRecorder()
	server.ServeHTTP(w, membershipPOST(t, "Bearer tok", body))
	if w.Code != http.StatusOK {
		t.Fatalf("status=%d", w.Code)
	}
	select {
	case change := <-got:
		if change.OperationID != "op-99" || change.ClusterID != "cl" || change.ExpectedConfigID != 3 || change.Remove != "n2" {
			t.Fatalf("decoded change=%+v", change)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("callback not invoked")
	}
}

package network

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/pkg/materializer"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

func TestVoterRecoveryStatusEndpoint(t *testing.T) {
	core := mustCore(t, "n1", []quepaxa.Member{{ID: "n1"}}, nil, nil)
	material, err := materializer.Open(t.TempDir()+"/sqlite.db", 1)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = material.Close() })
	server := NewServer(core, material, "cluster-a", true, nil)
	t.Cleanup(server.Close)
	server.SetVoterRecoveryStatus(func(ctx context.Context) VoterRecoveryStatus {
		_, _, err := core.ReadIndex(ctx)
		return VoterRecoveryStatus{
			NodeID: "n1", ClusterID: "cluster-a", Durability: "before-ack",
			Ready: true, Quorum: err == nil, CertifiedTip: uint64(core.Tip()),
			AppliedTip: material.Tip(), ArchiveTip: 0,
		}
	})

	response := httptest.NewRecorder()
	server.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/recovery/status", nil))
	if response.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
	var status VoterRecoveryStatus
	if err := json.NewDecoder(response.Body).Decode(&status); err != nil {
		t.Fatal(err)
	}
	if status.NodeID != "n1" || status.ClusterID != "cluster-a" || status.Durability != "before-ack" || !status.Ready || !status.Quorum || status.ArchiveTip != 0 {
		t.Fatalf("status=%+v", status)
	}
}

func TestVoterRecoveryStatusEndpointRequiresVoterCallback(t *testing.T) {
	server := NewServer(nil, nil, "cluster", false, nil)
	t.Cleanup(server.Close)

	response := httptest.NewRecorder()
	server.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/recovery/status", nil))
	if response.Code != http.StatusNotFound {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}

	response = httptest.NewRecorder()
	server.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/recovery/status", nil))
	if response.Code != http.StatusMethodNotAllowed || response.Header().Get("Allow") != http.MethodGet {
		t.Fatalf("status=%d allow=%q", response.Code, response.Header().Get("Allow"))
	}
}

func TestRecoveryArchiveEndpointRequiresBearerAndPublishes(t *testing.T) {
	server := NewServer(nil, nil, "cluster", false, nil)
	t.Cleanup(server.Close)
	called := false
	server.SetRecoveryArchive("admin-token", func(ctx context.Context) error {
		called = true
		deadline, ok := ctx.Deadline()
		if !ok || time.Until(deadline) > recoveryArchiveTimeout {
			t.Fatal("recovery archive callback has no bounded context")
		}
		return nil
	})

	for _, authorization := range []string{"", "Bearer wrong-token"} {
		response := httptest.NewRecorder()
		request := httptest.NewRequest(http.MethodPost, "/recovery/archive", nil)
		request.Header.Set("Authorization", authorization)
		server.ServeHTTP(response, request)
		if response.Code != http.StatusForbidden || called {
			t.Fatalf("authorization=%q status=%d called=%t", authorization, response.Code, called)
		}
	}

	response := httptest.NewRecorder()
	server.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/recovery/archive", nil))
	if response.Code != http.StatusMethodNotAllowed {
		t.Fatalf("method status=%d", response.Code)
	}

	response = httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/recovery/archive", nil)
	request.Header.Set("Authorization", "Bearer admin-token")
	server.ServeHTTP(response, request)
	if response.Code != http.StatusOK || !called || response.Body.String() != "{\"status\":\"archived\"}\n" {
		t.Fatalf("status=%d called=%t body=%s", response.Code, called, response.Body.String())
	}
}

func TestRecoveryArchiveEndpointRejectsEmptyConfiguredToken(t *testing.T) {
	server := NewServer(nil, nil, "cluster", false, nil)
	t.Cleanup(server.Close)
	server.SetRecoveryArchive("", func(context.Context) error {
		t.Fatal("empty token must not run recovery archive")
		return nil
	})
	response := httptest.NewRecorder()
	server.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/recovery/archive", nil))
	if response.Code != http.StatusForbidden {
		t.Fatalf("status=%d", response.Code)
	}
}

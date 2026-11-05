package network

import (
	"strings"
	"testing"

	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

func TestManagedGraphActualWireBudget(t *testing.T) {
	config := quepaxa.Cluster{ConfigID: 1, Members: []quepaxa.Member{{ID: "n1"}, {ID: "n2"}, {ID: "n3"}}}
	transport := NewTransport(types.ClusterID("cluster"), "n1", &config, "")
	defer transport.Close()
	transport.adminToken = "local-test-admin"
	if err := transport.ValidateManagedGraphWireBudget(); err != nil {
		t.Fatal(err)
	}
	transport.adminToken = strings.Repeat("x", maxPeerFrame)
	if err := transport.ValidateManagedGraphWireBudget(); err == nil {
		t.Fatal("oversized actual credential accepted")
	}
}

package operator

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/mrchypark/rhiza/internal/types"
)

func TestManagedBootstrapProfileInstalledMismatchBeforeWrites(t *testing.T) {
	for _, liveMixed := range []bool{false, true} {
		writes := 0
		cluster := phase8Cluster
		if liveMixed {
			cluster = strings.Replace(cluster, `"genesisIntent":"Create"`, `"genesisIntent":"Create","applicationProtocol":"rhiza-managed-sql-kv/v1"`, 1)
		}
		server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodGet {
				writes++
				t.Errorf("unexpected write %s", r.Method)
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(cluster))
		}))
		_, store, _ := phase8Setup(t, cluster, "")
		store.Kube.BaseURL, store.Kube.Client = server.URL, server.Client()
		profile := V1Alpha2RuntimeProfile{ApplicationProtocol: types.ManagedApplicationProtocolSQLKV, EvidenceFormat: V1Alpha2ManagedSQLKVEvidenceFormat}
		_, err := ReconcileV1Alpha2ManagedInitialization(context.Background(), store.Kube, phase2Ref, profile)
		server.Close()
		if err == nil || writes != 0 {
			t.Fatalf("mixed activation or mismatch accepted: %v writes=%d", err, writes)
		}
		want := "installed application protocol differs"
		if liveMixed {
			want = "mixed managed runtime activation"
		}
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("wrong refusal boundary: %v", err)
		}
	}
	b, err := json.Marshal(V1Alpha2RuntimeProfile{})
	if err != nil || strings.Contains(string(b), "ApplicationProtocol") {
		t.Fatalf("legacy profile serialization changed: %s %v", b, err)
	}
}

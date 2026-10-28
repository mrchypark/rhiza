package operator

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/mrchypark/rhiza/pkg/fencing"
)

func TestFencingExecutorRejectsMixedPayloadBeforeAuthenticationOrDispatch(t *testing.T) {
	tests := []struct {
		name    string
		message fencing.AuthorityMessage
	}{
		{
			name: "init begin plus phase prepare",
			message: fencing.AuthorityMessage{
				Action: "init-begin", BeginInit: &fencing.BeginInitRequest{AttemptID: "attempt"},
				RepairPhasePrepare: &fencing.MembershipRepairPhasePrepareRequest{OperationID: "repair"},
			},
		},
		{
			name: "runtime admission plus repair reserve",
			message: fencing.AuthorityMessage{
				Action: "bootstrap-runtime-admission", RuntimeAdmission: &fencing.BootstrapStatusRequest{OperationID: "dr"},
				RepairReserve: &fencing.MembershipRepairReserveRequest{TargetNodeID: "target"},
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			data, err := json.Marshal(test.message)
			if err != nil {
				t.Fatal(err)
			}
			req := httptest.NewRequest(http.MethodPost, "/v1/supervisor", bytes.NewReader(data))
			recorder := httptest.NewRecorder()
			(&FencingExecutor{}).Handler().ServeHTTP(recorder, req)
			if recorder.Code != http.StatusBadRequest {
				t.Fatalf("mixed payload reached authentication/dispatch: HTTP %d", recorder.Code)
			}
		})
	}
}

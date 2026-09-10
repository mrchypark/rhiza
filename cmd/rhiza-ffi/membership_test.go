//go:build cgo

package main

import (
	"encoding/json"
	"testing"
)

func TestMembershipDispatchAndStrictWireFormat(t *testing.T) {
	handle := openTestDB(t)
	for _, operation := range []string{"membership_change", "membership_abort", "membership_status"} {
		request := `{}`
		if operation != "membership_status" {
			request = `{"operation_id":"replace","cluster_id":"cluster","expected_config_id":2,"expected_abort_slot":17,"add":{"node_id":"new","peer_url":"quic://127.0.0.1:9000","token":"peer-token","wal_identity":"identity"}}`
		}
		payload, _ := json.Marshal(callEnvelope{Operation: operation, Request: json.RawMessage(request)})
		var response ffiEnvelope
		if err := json.Unmarshal(goCall(handle, payload, 1000), &response); err != nil {
			t.Fatal(err)
		}
		if response.Error == nil || response.Error.Code != "not_ready" {
			t.Fatalf("%s: %+v", operation, response.Error)
		}
	}
	payload := []byte(`{"operation":"membership_change","request":{"add":{"id":"wrong-field"}}}`)
	var response ffiEnvelope
	if err := json.Unmarshal(goCall(handle, payload, 1000), &response); err != nil {
		t.Fatal(err)
	}
	if response.Error == nil || response.Error.Code != "invalid_request" {
		t.Fatalf("strict member decode: %+v", response.Error)
	}
}

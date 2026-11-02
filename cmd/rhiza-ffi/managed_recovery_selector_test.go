//go:build cgo

package main

import "testing"

func TestManagedRecoverySelectorStrictPair(t *testing.T) {
	for _, tc := range []struct {
		name, input              string
		allowEmpty, valid, empty bool
	}{
		{"noarg", `{}`, true, true, true},
		{"status-needs-selector", `{}`, false, false, false},
		{"sql", `{"kind":"sql","request_id_b64":"eA=="}`, true, true, false},
		{"raw-id", `{"kind":"kv","request_id_b64":"/w=="}`, false, true, false},
		{"null-object", `null`, true, false, false},
		{"null-kind", `{"kind":null,"request_id_b64":"eA=="}`, true, false, false},
		{"null-id", `{"kind":"sql","request_id_b64":null}`, true, false, false},
		{"kind-only", `{"kind":"sql"}`, true, false, false},
		{"id-only", `{"request_id_b64":"eA=="}`, true, false, false},
		{"bad-kind", `{"kind":"graph","request_id_b64":"eA=="}`, true, false, false},
		{"bad-base64", `{"kind":"kv","request_id_b64":"!"}`, true, false, false},
		{"extra", `{"kind":"kv","request_id_b64":"eA==","original":{}}`, true, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			selector, err := decodeManagedRequestSelector([]byte(tc.input), tc.allowEmpty)
			if (err == nil) != tc.valid || tc.valid && (selector == nil) != tc.empty {
				t.Fatalf("selector validity/empty mismatch: error=%v", err)
			}
			if tc.name == "raw-id" && selector != nil && selector.RequestID != "\xff" {
				t.Fatal("raw ID changed")
			}
		})
	}
}
